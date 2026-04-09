"""Utilities for managing target environments for query deployment."""

import logging
import os
import re
import shutil
from functools import cache
from pathlib import Path
from typing import List, NamedTuple, Optional, Set

import attr
import cattrs
import click
import git
import google.auth.transport.requests
import jinja2
import requests
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from jinja2 import Template

from bigquery_etl.routine.parse_routine import (
    PERSISTENT_UDF_RE,
    ROUTINE_FILES,
    routine_usage_pattern,
)

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


def render_dataset_pattern(target: Target, branch: Optional[str] = None) -> str:
    """Render a target's dataset template into a regex pattern for matching dataset names.

    Known values (e.g. branch) are rendered literally; unknown or variable parts
    (commit, username, artifact ids) become ``[a-zA-Z0-9_]+`` regex wildcards.

    Returns a regex string anchored with ``^…$`` for ``dataset`` targets or
    ``^…`` for ``dataset_prefix`` targets.
    """
    template_str = target.dataset or target.dataset_prefix
    if not template_str:
        # No naming template — cannot determine which datasets belong to this target.
        raise click.ClickException(
            f"Target '{target.name}' has no dataset or dataset_prefix template. "
            "Cannot determine which datasets belong to this target."
        )

    _WILDCARD = "XBQETLWILDX"
    counter = [0]

    def _wc() -> str:
        counter[0] += 1
        return f"{_WILDCARD}{counter[0]}"

    git_ctx = {
        "branch": branch if branch else _wc(),
        "commit": _wc(),
    }
    account_ctx = {"username": _wc()}
    artifact_ctx = {"project_id": _wc(), "dataset_id": _wc()}

    rendered = Template(template_str).render(
        git=git_ctx,
        account=account_ctx,
        artifact=artifact_ctx,
    )

    sanitized = sanitize_bq_id(rendered)

    pattern = re.escape(sanitized)
    pattern = re.sub(rf"{_WILDCARD}\d+", r"[a-zA-Z0-9_]+", pattern)

    if target.dataset:
        return f"^{pattern}$"
    return f"^{pattern}"


def render_artifact_template(
    template: Optional[str], project_id: str, dataset_id: str = ""
) -> Optional[str]:
    """Render {{ artifact.* }} variables in a target config template string."""
    if not template or "{{" not in str(template):
        return template
    return Template(str(template), undefined=jinja2.StrictUndefined).render(
        artifact={
            "project_id": sanitize_bq_id(project_id or ""),
            "dataset_id": sanitize_bq_id(dataset_id or ""),
        }
    )


def _get_targets_file() -> Path:
    """Return the path to the targets config file (always in bigquery-etl)."""
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
            "commit": repo.active_branch.commit.hexsha,
        }
    except Exception:
        logging.warning(
            "Not in a git repository. Using 'unknown' for git.branch and git.commit"
        )
        return {"branch": "unknown", "commit": "unknown"}


@cache
def _get_gcloud_account() -> str:
    """Return the active GCP account email from credentials, cached after first call.

    Returns an empty string if the account cannot be determined.
    """
    try:
        client = bigquery.Client()
        credentials = client._credentials
        if not credentials.valid:
            credentials.refresh(google.auth.transport.requests.Request())
        return (
            requests.get(
                "https://oauth2.googleapis.com/tokeninfo",
                params={"access_token": credentials.token},
            )
            .json()
            .get("email", "")
        )
    except Exception as e:
        logging.warning(f"Could not determine GCP account from credentials: {e}")
        return ""


@cache
def _get_account_context() -> dict:
    """Return account template variables, cached after first call."""
    account = _get_gcloud_account()
    if account:
        username = account.split("@")[0]
    else:
        logging.warning(
            "Could not determine GCP account. Using 'unknown' for account.username"
        )
        username = "unknown"
    return {"username": username}


def get_target(target: str) -> Target:
    """Load and return a Target from the targets config file by name."""
    targets_file = _get_targets_file()

    if not targets_file.exists():
        raise Exception(f"Targets file not found: {targets_file}")

    template = Template(targets_file.read_text(), undefined=_KeepUndefined)
    rendered_content = template.render(
        git=_get_git_context(),
        account=_get_account_context(),
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

    targets = yaml.safe_load(targets_file.read_text())
    if isinstance(targets, dict):
        return targets.get("default_target")

    return None


def _artifact_exists(
    client: bigquery.Client,
    target_project: str,
    dataset: str,
    name: str,
    is_routine: bool,
) -> bool:
    """Return True if the artifact (table or routine) exists in BigQuery."""
    ref = f"{target_project}.{dataset}.{name}"
    try:
        if is_routine:
            client.get_routine(ref)
        else:
            client.get_table(ref)
        return True
    except NotFound:
        return False


def _get_deployed_artifacts_in_target(
    sql_dir: str,
    target_project: str,
    file_patterns: tuple,
    is_routine: bool = False,
) -> Set[DeployedTableInfo]:
    """Find artifacts deployed in the target directory.

    Scans for files matching file_patterns under <sql_dir>/<target_project>/,
    checks that the artifact exists in BigQuery, and reads the source manifest.
    """
    deployed: Set[DeployedTableInfo] = set()
    target_project_dir = Path(sql_dir) / target_project

    if not target_project_dir.exists():
        return deployed

    client = bigquery.Client(project=target_project)
    seen_dirs: Set[Path] = set()
    for pattern in file_patterns:
        for artifact_file in target_project_dir.rglob(pattern):
            if artifact_file.parent in seen_dirs:
                continue
            seen_dirs.add(artifact_file.parent)

            if artifact_file.parent.parent.parent != target_project_dir:
                continue

            name = artifact_file.parent.name
            dataset = artifact_file.parent.parent.name

            if not _artifact_exists(client, target_project, dataset, name, is_routine):
                continue

            manifest_file = artifact_file.parent / MANIFEST_FILENAME
            if not manifest_file.exists():
                if is_routine:
                    continue
                deployed.add(
                    DeployedTableInfo(
                        target_project=target_project,
                        target_dataset=dataset,
                        target_table=name,
                    )
                )
                continue

            try:
                manifest = yaml.safe_load(manifest_file.read_text())
            except Exception:
                continue

            source_project = manifest.get("source_project")
            source_dataset = manifest.get("source_dataset")
            source_table = manifest.get("source_table")

            if is_routine and not all([source_project, source_dataset, source_table]):
                continue

            deployed.add(
                DeployedTableInfo(
                    target_project=target_project,
                    target_dataset=dataset,
                    target_table=name,
                    source_project=source_project,
                    source_dataset=source_dataset,
                    source_table=source_table,
                )
            )

    return deployed


def get_deployed_tables_in_target(
    sql_dir: str, target_project: str
) -> Set[DeployedTableInfo]:
    """Find all tables deployed in the target directory."""
    return _get_deployed_artifacts_in_target(
        sql_dir, target_project, ("query.sql", "script.sql", "part1.sql")
    )


def get_deployed_routines_in_target(
    sql_dir: str, target_project: str
) -> Set[DeployedTableInfo]:
    """Find all routines deployed in the target directory."""
    return _get_deployed_artifacts_in_target(
        sql_dir, target_project, ROUTINE_FILES, is_routine=True
    )


def rewrite_query_references(
    query_file: Path,
    sql_dir: str,
    target_project: str,
    target: "Target",
    rewrite_all: bool = False,
):
    """Rewrite references in a query file to point to target environment."""
    sql = render_template(
        query_file.name, template_folder=str(query_file.parent), format=False
    )

    if rewrite_all:
        # rewrite ALL references to target, rendering target properties per matched
        # reference using its own source project/dataset
        gcp_project_pattern = (
            r"`?([a-z][a-z0-9\-]*[a-z0-9])`?\.`?([a-zA-Z0-9_]+)`?\.`?([a-zA-Z0-9_]+)`?"
        )

        def replace_all(m: re.Match) -> str:
            src_project, src_dataset, src_table = m.group(1), m.group(2), m.group(3)
            rendered_artifact_prefix = sanitize_bq_id(
                render_artifact_template(
                    target.artifact_prefix, src_project, src_dataset
                )
                or ""
            )
            if target.dataset:
                rendered = render_artifact_template(
                    target.dataset, src_project, src_dataset
                )
                target_ds = sanitize_bq_id(rendered) if rendered else src_dataset
            elif target.dataset_prefix:
                rendered = render_artifact_template(
                    target.dataset_prefix, src_project, src_dataset
                )
                prefix = sanitize_bq_id(rendered) if rendered else ""
                target_ds = sanitize_bq_id(f"{prefix}{src_dataset}")
            else:
                target_ds = src_dataset
            return f"`{target_project}`.`{target_ds}`.`{rendered_artifact_prefix}{src_table}`"

        sql = re.sub(gcp_project_pattern, replace_all, sql)
    else:
        # smart rewriting: only rewrite references to tables that exist in target
        deployed_tables = get_deployed_tables_in_target(sql_dir, target_project)

        def expected_target_dataset(info: DeployedTableInfo) -> Optional[str]:
            """Render the target dataset for a specific deployed artifact."""
            # source_project/source_dataset are guaranteed non-None by the caller
            assert info.source_project is not None
            assert info.source_dataset is not None
            if target.dataset:
                rendered = render_artifact_template(
                    target.dataset, info.source_project, info.source_dataset
                )
                return sanitize_bq_id(rendered) if rendered else None
            if target.dataset_prefix:
                rendered = render_artifact_template(
                    target.dataset_prefix, info.source_project, info.source_dataset
                )
                prefix = sanitize_bq_id(rendered) if rendered else ""
                return sanitize_bq_id(f"{prefix}{info.source_dataset}")
            # no dataset or dataset_prefix — target dataset equals source dataset
            return info.source_dataset

        for info in deployed_tables:
            if (
                info.source_project is None
                or info.source_dataset is None
                or info.source_table is None
            ):
                continue

            # Skip artifacts whose target dataset doesn't match what the current
            # target config would produce for that artifact's source — guards against
            # rewriting to stale deployments from a different branch/config.
            expected = expected_target_dataset(info)
            if expected is not None and info.target_dataset != expected:
                continue

            pattern = (
                rf"`?{re.escape(info.source_project)}`?"
                rf"\.`?{re.escape(info.source_dataset)}`?"
                rf"\.`?{re.escape(info.source_table)}\b`?"
            )
            replacement = (
                f"`{target_project}`.`{info.target_dataset}`.`{info.target_table}`"
            )
            sql = re.sub(pattern, replacement, sql)

        # Also rewrite references to routines deployed in the target.
        # Routine references can be 2-part (dataset.name) or 3-part
        # (project.dataset.name), so we use routine_usage_pattern which
        # handles both forms (matching only before a "(" call).
        deployed_routines = get_deployed_routines_in_target(sql_dir, target_project)

        for info in deployed_routines:
            if (
                info.source_project is None
                or info.source_dataset is None
                or info.source_table is None
            ):
                continue

            expected = expected_target_dataset(info)
            if expected is not None and info.target_dataset != expected:
                continue

            udf_pattern = routine_usage_pattern(
                f"{info.source_dataset}.{info.source_table}",
                info.source_project,
            )
            replacement = (
                f"`{target_project}`.`{info.target_dataset}`.`{info.target_table}`"
            )
            sql = udf_pattern.sub(replacement, sql)

    query_file.write_text(sql)


def prepare_target_directory(
    query_file: Path,
    sql_dir: str,
    target: "Target",
    defer_to_target: bool,
    isolated: bool,
    copied_target_dirs: Optional[Set[Path]] = None,
) -> Path:
    """Prepare target directory for query execution with --target."""
    source_project, source_dataset, source_table = extract_from_query_path(query_file)
    effective_project = target.project_id or source_project

    rendered_dataset = render_artifact_template(
        target.dataset, source_project, source_dataset
    )
    rendered_dataset_prefix = render_artifact_template(
        target.dataset_prefix, source_project, source_dataset
    )
    rendered_artifact_prefix = render_artifact_template(
        target.artifact_prefix, source_project, source_dataset
    )

    if rendered_dataset:
        effective_dataset = sanitize_bq_id(rendered_dataset)
    elif rendered_dataset_prefix:
        effective_dataset = sanitize_bq_id(f"{rendered_dataset_prefix}{source_dataset}")
    else:
        effective_dataset = source_dataset

    effective_table = (
        sanitize_bq_id(f"{rendered_artifact_prefix}{source_table}")
        if rendered_artifact_prefix
        else source_table
    )

    target_dir = Path(sql_dir) / effective_project / effective_dataset / effective_table
    target_query_file = target_dir / query_file.name

    if target_dir == query_file.parent:
        return query_file

    target_dir.mkdir(parents=True, exist_ok=True)

    # Copy source files and write the manifest once per target directory per
    # prepare_target_files() call, so reruns always get fresh source files while
    # multiple files from the same table don't overwrite each other's rewrites.
    if copied_target_dirs is None or target_dir not in copied_target_dirs:
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

        if copied_target_dirs is not None:
            copied_target_dirs.add(target_dir)

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

    # for routine files, rewrite the CREATE FUNCTION/PROCEDURE self-reference
    if target_query_file.name in ("udf.sql", "stored_procedure.sql"):
        sql = target_query_file.read_text()
        sql = PERSISTENT_UDF_RE.sub(
            rf"\g<prefix>`{effective_dataset}`.`{effective_table}`",
            sql,
            count=1,
        )
        target_query_file.write_text(sql)

    if defer_to_target or isolated:
        rewrite_query_references(
            target_query_file,
            sql_dir,
            effective_project,
            target,
            rewrite_all=isolated,
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
        user_email = _get_gcloud_account()

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
        deploy_table(artifact_file=query_file, destination_table=table_ref, force=True)
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
    copied_target_dirs: Set[Path] = set()
    target_query_files = [
        prepare_target_directory(
            query_file,
            sql_dir,
            target,
            defer_to_target,
            isolated,
            copied_target_dirs=copied_target_dirs,
        )
        for query_file in query_files
    ]

    if auto_deploy:
        for query_file in target_query_files:
            auto_deploy_if_needed(query_file, target.project_id or project_id)

    return target_query_files
