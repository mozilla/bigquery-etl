"""Utilities for managing target environments for query deployment."""

import logging
import os
import re
import shutil
import subprocess
from functools import cache
from pathlib import Path
from typing import List, NamedTuple, Optional, Set

import attr
import cattrs
import click
import git
import jinja2
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from jinja2 import Environment, Template

from ..config import ConfigLoader
from ..deploy import deploy_table
from . import extract_from_query_path
from .common import get_bqetl_project_root
from .common import render as render_template

ROOT = Path(__file__).parent.parent.parent

MANIFEST_FILENAME = ".bqetl_target_info.yaml"
DEFAULT_TARGETS_FILENAME = "bqetl_targets.yaml"


class DeployedTableInfo(NamedTuple):
    """Info about a table deployed to a target environment."""

    target_project: str
    target_dataset: str
    target_table: str
    source_project: Optional[str] = None
    source_dataset: Optional[str] = None
    source_table: Optional[str] = None


class _KeepUndefined(jinja2.Undefined):
    """Jinja2 Undefined that preserves {{ var.attr }} expressions in output instead of raising."""

    def __str__(self) -> str:
        return "{{ " + (self._undefined_name or "") + " }}"

    def __getattr__(self, name: str) -> "_KeepUndefined":
        if name.startswith("__"):
            raise AttributeError(name)
        return _KeepUndefined(name=f"{self._undefined_name}.{name}")

    def __getitem__(self, key: object) -> "_KeepUndefined":  # type: ignore[override]
        return _KeepUndefined(name=f"{self._undefined_name}[{key!r}]")


def sanitize_bq_id(value: str) -> str:
    """Sanitize a BigQuery identifier (project, dataset, or table) to conform to requirements.

    Identifiers must be alphanumeric (plus underscores) and at most 1024 characters.
    Replaces hyphens and other invalid characters with underscores.
    """
    # replace hyphens and other non-alphanumeric characters (except underscores) with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", value)
    return sanitized[:1024]


@attr.s()
class Target:
    """Target configuration for deployment."""

    name: str = attr.ib()
    project_id: str = attr.ib()
    dataset_prefix: Optional[str] = attr.ib(default=None)
    dataset: Optional[str] = attr.ib(default=None)
    artifact_prefix: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        """Check attributes."""
        if self.dataset is not None and self.dataset_prefix is not None:
            raise ValueError(
                "Cannot specify both 'dataset' and 'dataset_prefix' in a target"
            )


def render_artifact_template(
    template: Optional[str], project_id: str, dataset_id: str = ""
) -> Optional[str]:
    """Render {{ artifact.* }} variables in a target config template string."""
    if not template or "{{" not in str(template):
        return template
    return Template(str(template)).render(
        artifact={
            "project_id": sanitize_bq_id(project_id or ""),
            "dataset_id": sanitize_bq_id(dataset_id or ""),
        }
    )


def _get_targets_file() -> Path:
    """Return the path to the targets config file."""
    targets_file_name = ConfigLoader.get(
        "default", "targets", fallback=DEFAULT_TARGETS_FILENAME
    )
    return ConfigLoader.project_dir / targets_file_name


@cache
def _get_git_context() -> dict:
    """Return git template variables, cached after first call."""
    try:
        project_root = get_bqetl_project_root() or ROOT
        repo = git.Repo(project_root)
        return {
            "branch": repo.active_branch.name,
            "commit": repo.active_branch.commit.hexsha[:8],
        }
    except Exception:
        logging.warning(
            "Not in a git repository. Using 'unknown' for git.branch and git.commit"
        )
        return {"branch": "unknown", "commit": "unknown"}


@cache
def _get_user_context() -> dict:
    """Return user template variables, cached after first call."""
    try:
        result = subprocess.run(
            ["gcloud", "config", "get", "account"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        account = result.stdout.strip()
        user_name = account.split("@")[0] if "@" in account else account
    except Exception:
        logging.warning(
            "Could not determine GCP account. Using 'unknown' for user.name"
        )
        user_name = "unknown"
    return {"name": user_name}


def get_target(target: str) -> Target:
    """Load and return a Target from the targets config file by name."""
    targets_file = _get_targets_file()

    if not targets_file.exists():
        raise Exception(f"Targets file not found: {targets_file}")

    targets_content = targets_file.read_text()
    env = Environment(undefined=_KeepUndefined)
    template = env.from_string(targets_content)
    rendered_content = template.render(
        git=_get_git_context(),
        user=_get_user_context(),
    )

    targets = yaml.safe_load(rendered_content)

    if isinstance(targets, dict) and target in targets:
        return cattrs.structure({**targets[target], "name": target}, Target)

    raise Exception(f"Couldn't find target `{target}` in {targets_file}")


def get_default_target_name() -> Optional[str]:
    """Return the default target name, checking sources in priority order.

    1. BQETL_TARGET environment variable
    2. default_target key in bqetl_targets.yaml
    """
    env_target = os.environ.get("BQETL_TARGET")
    if env_target:
        return env_target

    targets_file = _get_targets_file()

    if not targets_file.exists():
        return None

    try:
        targets = yaml.safe_load(targets_file.read_text())
        if isinstance(targets, dict):
            return targets.get("default_target")
    except Exception:
        return None

    return None


def get_deployed_tables_in_target(
    sql_dir: str, target_project: str
) -> Set[DeployedTableInfo]:
    """Find all tables deployed in the target directory."""
    deployed: Set[DeployedTableInfo] = set()
    target_project_dir = Path(sql_dir) / target_project

    if not target_project_dir.exists():
        return deployed

    seen_dirs: Set[Path] = set()
    for pattern in ("query.sql", "script.sql", "part*.sql"):
        for query_file in target_project_dir.rglob(pattern):
            if query_file.parent in seen_dirs:
                continue
            seen_dirs.add(query_file.parent)

            if query_file.parent.parent.parent != target_project_dir:
                continue

            table = query_file.parent.name
            dataset = query_file.parent.parent.name

            source_project = None
            source_dataset = None
            source_table = None
            manifest_file = query_file.parent / MANIFEST_FILENAME
            if manifest_file.exists():
                try:
                    manifest = yaml.safe_load(manifest_file.read_text())
                    source_project = manifest.get("source_project")
                    source_dataset = manifest.get("source_dataset")
                    source_table = manifest.get("source_table")
                except Exception:
                    pass

            deployed.add(
                DeployedTableInfo(
                    target_project=target_project,
                    target_dataset=dataset,
                    target_table=table,
                    source_project=source_project,
                    source_dataset=source_dataset,
                    source_table=source_table,
                )
            )

    return deployed


def rewrite_query_references(
    query_file: Path,
    sql_dir: str,
    target_project: str,
    dataset_prefix: Optional[str],
    rewrite_all: bool = False,
    dataset: Optional[str] = None,
    artifact_prefix: Optional[str] = None,
):
    """Rewrite references in a query file to point to target environment."""
    sql = render_template(
        query_file.name, template_folder=str(query_file.parent), format=False
    )

    if rewrite_all:
        # rewrite ALL references to target
        # this rewrites references from ANY GCP project to target project with prefix
        gcp_project_pattern = (
            r"`?([a-z][a-z0-9\-]*[a-z0-9])`?\.`?([a-zA-Z0-9_]+)`?\.`?([a-zA-Z0-9_]+)`?"
        )
        effective_dataset = sanitize_bq_id(dataset) if dataset else None
        sanitized_artifact_prefix = (
            sanitize_bq_id(artifact_prefix) if artifact_prefix else ""
        )

        def replace_all(m: re.Match) -> str:
            target_ds = effective_dataset or (
                sanitize_bq_id(f"{dataset_prefix}{m.group(2)}")
                if dataset_prefix
                else m.group(2)
            )
            return f"`{target_project}`.`{target_ds}`.`{sanitized_artifact_prefix}{m.group(3)}`"

        sql = re.sub(gcp_project_pattern, replace_all, sql)
    else:
        # smart rewriting: only rewrite references to tables that exist in target
        deployed_tables = get_deployed_tables_in_target(sql_dir, target_project)

        # filter to tables that belong to the current target configuration to avoid
        # incorrectly rewriting references to tables from a different configuration
        # (e.g. a different branch) that happen to share the same target project
        sanitized_dataset = sanitize_bq_id(dataset) if dataset else None
        sanitized_dataset_prefix = (
            sanitize_bq_id(dataset_prefix) if dataset_prefix else None
        )
        sanitized_artifact_prefix = (
            sanitize_bq_id(artifact_prefix) if artifact_prefix else ""
        )

        def matches_target_config(info: DeployedTableInfo) -> bool:
            if sanitized_dataset and info.target_dataset != sanitized_dataset:
                return False
            if sanitized_dataset_prefix and not info.target_dataset.startswith(
                sanitized_dataset_prefix
            ):
                return False
            if sanitized_artifact_prefix and not info.target_table.startswith(
                sanitized_artifact_prefix
            ):
                return False
            return True

        for info in filter(matches_target_config, deployed_tables):
            if (
                info.source_project is None
                or info.source_dataset is None
                or info.source_table is None
            ):
                continue

            pattern = (
                rf"`?{re.escape(info.source_project)}`?"
                rf"\.`?{re.escape(info.source_dataset)}`?"
                rf"\.`?{re.escape(info.source_table)}`?"
            )
            replacement = (
                f"`{target_project}`.`{info.target_dataset}`.`{info.target_table}`"
            )
            sql = re.sub(pattern, replacement, sql)

    query_file.write_text(sql)


def prepare_target_directory(
    query_file: Path,
    sql_dir: str,
    destination_project_id: Optional[str],
    dataset_prefix: Optional[str],
    defer_to_target: bool,
    isolated: bool,
    dataset: Optional[str] = None,
    artifact_prefix: Optional[str] = None,
) -> Optional[Path]:
    """Prepare target directory for query execution with --target."""
    if (
        not destination_project_id
        and not dataset_prefix
        and not dataset
        and not artifact_prefix
    ):
        return None

    source_project, source_dataset, source_table = extract_from_query_path(query_file)
    effective_project = destination_project_id or source_project

    if dataset:
        effective_dataset = sanitize_bq_id(dataset)
    elif dataset_prefix:
        effective_dataset = sanitize_bq_id(f"{dataset_prefix}{source_dataset}")
    else:
        effective_dataset = source_dataset

    effective_table = (
        sanitize_bq_id(f"{artifact_prefix}{source_table}")
        if artifact_prefix
        else source_table
    )

    target_dir = Path(sql_dir) / effective_project / effective_dataset / effective_table
    target_query_file = target_dir / query_file.name

    target_dir.mkdir(parents=True, exist_ok=True)

    # Only copy files and write the manifest once per directory. If the manifest
    # already exists, the directory was set up by a previous call (e.g. another
    # part file from the same table), and copying again would overwrite already-
    # rewritten files.
    if not (target_dir / MANIFEST_FILENAME).exists():
        source_dir = query_file.parent
        for item in source_dir.iterdir():
            if item.is_file():
                shutil.copy2(item, target_dir / item.name)

        manifest = {
            "source_project": source_project,
            "source_dataset": source_dataset,
            "source_table": source_table,
        }
        (target_dir / MANIFEST_FILENAME).write_text(yaml.dump(manifest))

    # for view and materialized view files, always rewrite the CREATE OR REPLACE VIEW
    # self-reference to match the target directory structure
    if target_query_file.name in ("view.sql", "materialized_view.sql"):
        sql = target_query_file.read_text()
        sql = re.sub(
            r"(CREATE\s+OR\s+REPLACE\s+VIEW\s+)`?[^`.\s]+"
            r"`?\.`?[^`.\s]+`?\.`?[^`.\s]+`?",
            rf"\1`{effective_project}.{effective_dataset}.{effective_table}`",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        target_query_file.write_text(sql)

    if defer_to_target or isolated:
        rewrite_query_references(
            target_query_file,
            sql_dir,
            effective_project,
            dataset_prefix,
            rewrite_all=isolated,
            dataset=dataset,
            artifact_prefix=artifact_prefix,
        )

    return target_query_file


def ensure_dataset_exists(client: bigquery.Client, dataset_ref: str) -> bool:
    """Create a dataset if it doesn't exist, with user-only access permissions."""
    try:
        client.get_dataset(dataset_ref)
        return True
    except NotFound:
        pass

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    try:
        result = subprocess.run(
            ["gcloud", "config", "get", "account"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        user_email = result.stdout.strip()

        # Explicitly grant ownership to the user. When running as yourself this
        # is redundant (BigQuery auto-grants dataOwner to the creator), but with
        # service account impersonation the SA would become the owner instead.
        if user_email and "@" in user_email:
            dataset.access_entries = [
                bigquery.AccessEntry(
                    role="OWNER",
                    entity_type="userByEmail",
                    entity_id=user_email,
                )
            ]
        else:
            click.echo("⚠️  Could not determine user email, using default permissions")
    except Exception as e:
        click.echo(f"⚠️  Could not set dataset permissions: {e}")

    try:
        client.create_dataset(dataset, exists_ok=True)
        click.echo(f"✅ Created dataset: {dataset_ref}")
        return True
    except Exception as e:
        click.echo(f"⚠️  Failed to create dataset: {e}")
        return False


def auto_deploy_if_needed(
    query_file: Path,
    target_project: str,
):
    """Automatically deploy table schema, creating or updating as needed."""
    _, target_dataset, table_name = extract_from_query_path(query_file)

    client = bigquery.Client(project=target_project)
    dataset_ref = f"{target_project}.{target_dataset}"
    table_ref = f"{dataset_ref}.{table_name}"

    if not ensure_dataset_exists(client, dataset_ref):
        return

    try:
        deploy_table(
            artifact_file=query_file,
            destination_table=table_ref,
        )
        click.echo(f"✅ Deployed: {table_ref}")
    except Exception as e:
        click.echo(f"⚠️  Deployment failed: {e}")


def prepare_target_files(
    query_files: List[Path],
    sql_dir: str,
    project_id: str,
    target: "Target",
    defer_to_target: bool,
    isolated: bool,
    auto_deploy: bool = True,
) -> List[Path]:
    """Prepare target directories for multiple query files."""
    # resolve source_project for {{ artifact.* }} template rendering below.
    source_project = project_id
    if not source_project and query_files:
        try:
            source_project, _, _ = extract_from_query_path(query_files[0])
        except Exception:
            pass

    target_query_files = []
    for query_file in query_files:
        query_file_project = query_file.parent.parent.parent.name
        if query_file_project == (target.project_id or project_id):
            target_query_files.append(query_file)
        else:
            try:
                _, source_dataset, _ = extract_from_query_path(query_file)
            except Exception:
                source_dataset = ""

            rendered_dataset_prefix = render_artifact_template(
                target.dataset_prefix, source_project or "", source_dataset
            )
            rendered_dataset = render_artifact_template(
                target.dataset, source_project or "", source_dataset
            )
            rendered_artifact_prefix = render_artifact_template(
                target.artifact_prefix, source_project or "", source_dataset
            )

            target_file = prepare_target_directory(
                query_file,
                sql_dir,
                target.project_id,
                rendered_dataset_prefix,
                defer_to_target,
                isolated,
                dataset=rendered_dataset,
                artifact_prefix=rendered_artifact_prefix,
            )
            target_query_files.append(target_file if target_file else query_file)

    if auto_deploy:
        for query_file in target_query_files:
            auto_deploy_if_needed(query_file, target.project_id or project_id)

    return target_query_files
