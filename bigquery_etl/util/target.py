"""Utilities for managing target environments for query deployment."""

import logging
import re
import shutil
import subprocess
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
from .common import render as render_template

ROOT = Path(__file__).parent.parent.parent

MANIFEST_FILENAME = ".bqetl_target_info.yaml"


class DeployedTableInfo(NamedTuple):
    """Info about a table deployed to a target environment."""

    target_project: str
    target_dataset: str
    target_table: str
    source_dataset: Optional[str] = (
        None  # populated from manifest; None for legacy deployments
    )
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


def sanitize_dataset_id(dataset_id: str) -> str:
    """Sanitize dataset ID to conform to BigQuery requirements.

    Dataset IDs must be alphanumeric (plus underscores) and at most 1024 characters.
    Replaces hyphens and other invalid characters with underscores.
    """
    # replace hyphens and other non-alphanumeric characters (except underscores) with underscores
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", dataset_id)
    return sanitized[:1024]


@attr.s()
class Target:
    """Target configuration for deployment."""

    name: str = attr.ib()
    project_id: str = attr.ib()
    dataset_prefix: Optional[str] = attr.ib(default=None)
    dataset: Optional[str] = attr.ib(default=None)
    table_prefix: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        if self.dataset is not None and self.dataset_prefix is not None:
            raise ValueError(
                "Cannot specify both 'dataset' and 'dataset_prefix' in a target"
            )


def render_dataset_prefix(
    dataset_prefix: Optional[str], project_id: str
) -> Optional[str]:
    """Render {{ param }} in a dataset_prefix template."""
    if not dataset_prefix or "{{" not in str(dataset_prefix):
        return dataset_prefix
    return Template(str(dataset_prefix)).render(
        artifact={"project_id": sanitize_dataset_id(project_id or "")}
    )


def get_target(target: str) -> Target:
    """Load and return a Target from the targets config file by name."""
    targets_file_name = ConfigLoader.get(
        "default", "targets", fallback="bqetl_targets.yaml"
    )
    targets_file = ConfigLoader.project_dir / targets_file_name

    if not targets_file.exists():
        raise Exception(f"Targets file not found: {targets_file}")

    try:
        repo = git.Repo(ROOT)
        git_branch = repo.active_branch.name
        git_commit = repo.active_branch.commit.hexsha[:8]
    except Exception:
        logging.warning(
            "Not in a git repository. Using 'unknown' for git.branch and git.commit"
        )
        git_branch = "unknown"
        git_commit = "unknown"

    targets_content = targets_file.read_text()
    env = Environment(undefined=_KeepUndefined)
    template = env.from_string(targets_content)
    rendered_content = template.render(git={"branch": git_branch, "commit": git_commit})

    targets = yaml.safe_load(rendered_content)

    if isinstance(targets, dict) and target in targets:
        return cattrs.structure({**targets[target], "name": target}, Target)

    raise Exception(f"Couldn't find target `{target}` in {targets_file}")


def get_deployed_tables_in_target(
    sql_dir: str, target_project: str
) -> Set[DeployedTableInfo]:
    """Find all tables deployed in the target directory."""
    deployed: Set[DeployedTableInfo] = set()
    target_project_dir = Path(sql_dir) / target_project

    if not target_project_dir.exists():
        return deployed

    for query_file in target_project_dir.rglob("query.sql"):
        parts = query_file.parts
        if len(parts) >= 3:
            table = parts[-2]
            dataset = parts[-3]

            source_dataset = None
            source_table = None
            manifest_file = query_file.parent / MANIFEST_FILENAME
            if manifest_file.exists():
                try:
                    manifest = yaml.safe_load(manifest_file.read_text())
                    source_dataset = manifest.get("source_dataset")
                    source_table = manifest.get("source_table")
                except Exception:
                    pass

            deployed.add(
                DeployedTableInfo(
                    target_project=target_project,
                    target_dataset=dataset,
                    target_table=table,
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
    table_prefix: Optional[str] = None,
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
        effective_dataset = sanitize_dataset_id(dataset) if dataset else None
        sanitized_table_prefix = (
            sanitize_dataset_id(table_prefix) if table_prefix else ""
        )

        def replace_all(m: re.Match) -> str:
            target_ds = effective_dataset or (
                sanitize_dataset_id(f"{dataset_prefix}{m.group(2)}")
                if dataset_prefix
                else m.group(2)
            )
            return f"`{target_project}`.`{target_ds}`.`{sanitized_table_prefix}{m.group(3)}`"

        sql = re.sub(gcp_project_pattern, replace_all, sql)
    else:
        # smart rewriting: only rewrite references to tables that exist in target
        deployed_tables = get_deployed_tables_in_target(sql_dir, target_project)

        for info in deployed_tables:
            # Use manifest data when available for precise matching
            if info.source_dataset is not None and info.source_table is not None:
                original_dataset = info.source_dataset
                original_table = info.source_table
            else:
                # Legacy deployments without manifest: fall back to prefix-stripping
                original_dataset = info.target_dataset
                original_table = info.target_table
                if dataset_prefix:
                    sanitized_prefix = sanitize_dataset_id(dataset_prefix)
                    if original_dataset.startswith(sanitized_prefix):
                        original_dataset = original_dataset[len(sanitized_prefix) :]
                if table_prefix:
                    sanitized_tprefix = sanitize_dataset_id(table_prefix)
                    if original_table.startswith(sanitized_tprefix):
                        original_table = original_table[len(sanitized_tprefix) :]

            # rewrite fully qualified references from any project
            # any-project.dataset.table -> target_project.target_dataset.target_table
            pattern = rf"`?[a-z][a-z0-9\-]*[a-z0-9]`?\.`?{re.escape(original_dataset)}`?\.`?{re.escape(original_table)}`?"
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
    defer: bool,
    isolated: bool,
    dataset: Optional[str] = None,
    table_prefix: Optional[str] = None,
) -> Optional[Path]:
    """Prepare target directory for query execution with --target."""
    if (
        not destination_project_id
        and not dataset_prefix
        and not dataset
        and not table_prefix
    ):
        return None

    source_project, source_dataset, source_table = extract_from_query_path(query_file)
    effective_project = destination_project_id or source_project

    if dataset:
        effective_dataset = sanitize_dataset_id(dataset)
    elif dataset_prefix:
        effective_dataset = sanitize_dataset_id(f"{dataset_prefix}{source_dataset}")
    else:
        effective_dataset = source_dataset

    effective_table = (
        sanitize_dataset_id(f"{table_prefix}{source_table}")
        if table_prefix
        else source_table
    )

    target_dir = Path(sql_dir) / effective_project / effective_dataset / effective_table
    target_query_file = target_dir / query_file.name

    target_dir.mkdir(parents=True, exist_ok=True)
    source_dir = query_file.parent
    for item in source_dir.iterdir():
        if item.is_file():
            shutil.copy2(item, target_dir / item.name)

    # Write manifest so --defer rewriting can reverse-map deployed → original location
    manifest = {
        "source_project": source_project,
        "source_dataset": source_dataset,
        "source_table": source_table,
    }
    (target_dir / MANIFEST_FILENAME).write_text(yaml.dump(manifest))

    # for view files, always rewrite the CREATE OR REPLACE VIEW self-reference to
    # match the target directory structure
    if target_query_file.name == "view.sql":
        sql = target_query_file.read_text()
        sql = re.sub(
            r"(CREATE\s+OR\s+REPLACE\s+VIEW\s+)`?[^`\s]+"
            r"`?\.`?[^`\s]+`?\.`?[^`\s]+`?",
            rf"\1`{effective_project}.{effective_dataset}.{effective_table}`",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        target_query_file.write_text(sql)

    if defer or isolated:
        rewrite_query_references(
            target_query_file,
            sql_dir,
            effective_project,
            dataset_prefix,
            rewrite_all=isolated,
            dataset=dataset,
            table_prefix=table_prefix,
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
            ["gcloud", "config", "get-value", "account"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        user_email = result.stdout.strip()

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
    """Automatically deploy table schema if it doesn't exist in BigQuery."""
    _, target_dataset, table_name = extract_from_query_path(query_file)

    client = bigquery.Client(project=target_project)
    dataset_ref = f"{target_project}.{target_dataset}"
    table_ref = f"{dataset_ref}.{table_name}"

    if not ensure_dataset_exists(client, dataset_ref):
        return

    try:
        client.get_table(table_ref)
        click.echo(f"✅ Table exists: {table_ref}")
    except NotFound:
        try:
            deploy_table(
                artifact_file=query_file,
                destination_table=table_ref,
            )
            click.echo(f"✅ Deployed: {table_ref}")
        except Exception as e:
            click.echo(f"⚠️  Deployment skipped or failed: {e}")


def prepare_target_files(
    query_files: List[Path],
    sql_dir: str,
    project_id: str,
    destination_project_id: Optional[str],
    dataset_prefix: Optional[str],
    defer: bool,
    isolated: bool,
    auto_deploy: bool = True,
    dataset: Optional[str] = None,
    table_prefix: Optional[str] = None,
) -> List[Path]:
    """Prepare target directories for multiple query files."""
    if (
        not destination_project_id
        and not dataset_prefix
        and not dataset
        and not table_prefix
    ):
        return query_files

    # render {{ artifact.project_id }} in dataset_prefix/dataset using the source project.
    source_project = project_id
    if not source_project and query_files:
        try:
            source_project, _, _ = extract_from_query_path(query_files[0])
        except Exception:
            pass

    if dataset_prefix and "{{" in str(dataset_prefix):
        dataset_prefix = render_dataset_prefix(dataset_prefix, source_project or "")
    if dataset and "{{" in str(dataset):
        dataset = render_dataset_prefix(dataset, source_project or "")

    target_query_files = []
    for query_file in query_files:
        query_file_project = query_file.parent.parent.parent.name
        if query_file_project == (destination_project_id or project_id):
            target_query_files.append(query_file)
        else:
            target_file = prepare_target_directory(
                query_file,
                sql_dir,
                destination_project_id,
                dataset_prefix,
                defer,
                isolated,
                dataset=dataset,
                table_prefix=table_prefix,
            )
            target_query_files.append(target_file if target_file else query_file)

    if auto_deploy and (
        destination_project_id or dataset_prefix or dataset or table_prefix
    ):
        for query_file in target_query_files:
            auto_deploy_if_needed(
                query_file,
                destination_project_id or project_id,
            )

    return target_query_files
