"""Commands for managing target environments."""

import logging
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..cli.utils import sql_dir_option
from ..util.common import block_coding_agents
from ..util.target import render_artifact_prefix_pattern, render_dataset_pattern

log = logging.getLogger(__name__)


def _parse_duration(value: str) -> timedelta:
    """Parse a duration string like '7d', '24h', '2w', '30m' into a timedelta."""
    match = re.match(r"^(\d+)([mhdw])$", value)
    if not match:
        raise click.BadParameter(
            f"Invalid duration '{value}'. Use format like '7d', '24h', '2w', '30m'."
        )
    amount = int(match.group(1))
    unit = match.group(2)
    units = {"m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
    return timedelta(**{units[unit]: amount})


@click.group(help="Commands for managing target environments.")
@click.pass_context
def target(ctx):
    """Target management commands. Requires --target."""
    if not ctx.obj.get("target"):
        raise click.UsageError(
            "The --target flag is required for 'target' commands.\n"
            "Usage: ./bqetl --target <name> target <command> [OPTIONS]"
        )


@target.command(help="""Clean up target environment deployments in BigQuery.

    Discovers datasets in the target project that match the target's naming
    pattern and deletes stale artifacts. The cleanup granularity (dataset-level
    vs table-level) is determined automatically from the target configuration
    in bqetl_targets.yaml.

    --branch deletes artifacts for a specific branch. If git.branch is in the
    dataset/dataset_prefix template, entire matching datasets are deleted. If
    git.branch is in artifact_prefix, only matching tables within datasets are
    deleted. Both filters apply when branch appears in both templates.

    --older-than performs table-level cleanup: within matched datasets, only
    tables not updated within the given duration are deleted.

    --all deletes all matching datasets for this target.

    Empty datasets are removed automatically after table-level cleanup.

    Examples:

      ./bqetl --target dev target clean --older-than 7d

      ./bqetl --target dev target clean --branch feature-xyz

      ./bqetl --target dev target clean --all

      ./bqetl --target dev target clean --branch feature-xyz --older-than 7d

      ./bqetl --target dev target clean --older-than 7d --dry-run
    """)
@click.option(
    "--older-than",
    "older_than",
    default=None,
    help="Delete tables not updated within the given duration (e.g., 7d, 24h, 2w). Empty datasets are removed automatically.",
)
@click.option(
    "--branch",
    default=None,
    help="Delete deployments for a specific branch.",
)
@click.option(
    "--all",
    "all_deployments",
    is_flag=True,
    default=False,
    help="Delete all deployments for this target.",
)
@click.option(
    "--dry-run/--no-dry-run",
    "--dry_run/--no_dry_run",
    default=False,
    help="Show what would be deleted without actually deleting.",
)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Skip confirmation prompt.",
)
@sql_dir_option
@block_coding_agents
@click.pass_context
def clean(ctx, older_than, branch, all_deployments, dry_run, yes, sql_dir):
    """Clean up target environment deployments."""
    if not any([older_than, branch, all_deployments]):
        raise click.UsageError(
            "Must specify at least one filter: --older-than, --branch, or --all"
        )
    if branch and all_deployments:
        raise click.UsageError("--branch and --all are mutually exclusive.")

    target_config = ctx.obj["target"]
    project_id = target_config.project_id

    # build filters from target config
    dataset_re = re.compile(render_dataset_pattern(target_config, branch=branch))
    cutoff = (
        datetime.now(timezone.utc) - _parse_duration(older_than) if older_than else None
    )
    artifact_re = None
    if (
        branch
        and target_config.raw_artifact_prefix
        and "git.branch" in target_config.raw_artifact_prefix
    ):
        artifact_re = re.compile(
            render_artifact_prefix_pattern(target_config, branch=branch)
        )

    client = bigquery.Client(project=project_id)
    all_datasets = list(client.list_datasets())

    if not all_datasets:
        click.echo(f"No datasets found in project {project_id}.")
        return

    matching = [ds for ds in all_datasets if dataset_re.match(ds.dataset_id)]
    if not matching:
        click.echo("No matching deployments found.")
        return

    # table-level cleanup when we have table-level filters
    if artifact_re or cutoff:
        tables_by_ds = _find_matching_tables(
            client, project_id, matching, artifact_re, cutoff
        )
        if not tables_by_ds:
            click.echo("No matching tables found.")
            return

        total = sum(len(tbls) for tbls in tables_by_ds.values())
        click.echo(f"\nFound {total} table(s) to delete in {project_id}:\n")
        for dataset_id in sorted(tables_by_ds):
            click.echo(f"  {dataset_id}:")
            for tbl in sorted(tables_by_ds[dataset_id], key=lambda t: t.table_id):
                click.echo(f"    {tbl.table_id}")
        click.echo()

        if not _confirm(total, "table(s)", dry_run, yes):
            return

        deleted, empty = _delete_tables(client, project_id, tables_by_ds, sql_dir)
        click.echo(f"\nDeleted {deleted}/{total} table(s).")
        if empty:
            click.echo(f"Removed {len(empty)} empty dataset(s): " + ", ".join(empty))
        return

    # Dataset-level cleanup: --branch (when branch is only in dataset) or --all
    click.echo(f"\nFound {len(matching)} dataset(s) to delete in {project_id}:\n")
    for ds in sorted(matching, key=lambda d: d.dataset_id):
        click.echo(f"  {ds.dataset_id}")
    click.echo()

    if not _confirm(len(matching), "dataset(s)", dry_run, yes):
        return

    deleted = _delete_datasets(client, project_id, matching, sql_dir)
    click.echo(f"\nDeleted {deleted}/{len(matching)} dataset(s).")


def _find_matching_tables(client, project_id, datasets, artifact_re, cutoff):
    """Find tables within datasets that match the given filters.

    Returns a dict of dataset_id -> list of table references.
    """
    result = {}
    for ds in datasets:
        try:
            tables = list(client.list_tables(f"{project_id}.{ds.dataset_id}"))
        except NotFound:
            continue
        matched = []
        for tbl in tables:
            if artifact_re and not artifact_re.match(tbl.table_id):
                continue
            if cutoff:
                try:
                    table = client.get_table(tbl.reference)
                    if not (table.modified and table.modified < cutoff):
                        continue
                except NotFound:
                    continue
            matched.append(tbl)
        if matched:
            result[ds.dataset_id] = matched
    return result


def _delete_tables(client, project_id, tables_by_dataset, sql_dir):
    """Delete tables and clean up empty datasets and local files.

    Returns (deleted_count, empty_dataset_ids).
    """
    deleted = 0
    empty_datasets = []
    for dataset_id, tables in tables_by_dataset.items():
        for tbl in tables:
            try:
                client.delete_table(tbl.reference, not_found_ok=True)
                click.echo(f"  Deleted {dataset_id}.{tbl.table_id}")
                deleted += 1
            except Exception as e:
                click.echo(
                    f"  Failed to delete {dataset_id}.{tbl.table_id}: {e}",
                    err=True,
                )
            local_dir = Path(sql_dir) / project_id / dataset_id / tbl.table_id
            if local_dir.exists():
                shutil.rmtree(local_dir)

        # remove dataset if now empty
        dataset_ref = f"{project_id}.{dataset_id}"
        if not list(client.list_tables(dataset_ref)):
            try:
                client.delete_dataset(dataset_ref, not_found_ok=True)
                empty_datasets.append(dataset_id)
            except Exception:
                pass
            local_ds_dir = Path(sql_dir) / project_id / dataset_id
            if local_ds_dir.exists():
                shutil.rmtree(local_ds_dir)

    return deleted, empty_datasets


def _delete_datasets(client, project_id, datasets, sql_dir):
    """Delete entire datasets and local files. Returns deleted count."""
    deleted = 0
    for ds in datasets:
        dataset_ref = f"{project_id}.{ds.dataset_id}"
        try:
            client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
            click.echo(f"  Deleted {ds.dataset_id}")
            deleted += 1
        except Exception as e:
            click.echo(f"  Failed to delete {ds.dataset_id}: {e}", err=True)
        local_dir = Path(sql_dir) / project_id / ds.dataset_id
        if local_dir.exists():
            shutil.rmtree(local_dir)
    return deleted


def _confirm(count, label, dry_run, yes):
    """Prompt for confirmation. Returns True to proceed."""
    if dry_run:
        click.echo(f"Dry run: no {label} were deleted.")
        return False
    if not yes and not click.confirm(f"Delete {count} {label}?"):
        click.echo("Aborted.")
        return False
    return True
