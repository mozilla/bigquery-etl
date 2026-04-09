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
from ..util.target import render_dataset_pattern

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


@target.command(
    help="""Clean up target environment deployments in BigQuery.

    Discovers and deletes datasets in the target project that match the target's
    naming pattern. Use --branch, --older-than, or --all to select which
    deployments to clean up.

    Examples:

      ./bqetl --target dev target clean --older-than 7d

      ./bqetl --target dev target clean --branch feature-xyz

      ./bqetl --target dev target clean --all

      ./bqetl --target dev target clean --older-than 7d --dry-run
    """
)
@click.option(
    "--older-than",
    "older_than",
    default=None,
    help="Delete deployments older than the given duration (e.g., 7d, 24h, 2w).",
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

    max_age = _parse_duration(older_than) if older_than else None

    # Build regex pattern: --branch pins the branch, everything else wildcarded
    pattern = render_dataset_pattern(target_config, branch=branch)
    dataset_re = re.compile(pattern)

    client = bigquery.Client(project=target_config.project_id)
    all_datasets = list(client.list_datasets())

    if not all_datasets:
        click.echo(f"No datasets found in project {target_config.project_id}.")
        return

    # Match datasets by naming pattern
    matching = [ds for ds in all_datasets if dataset_re.match(ds.dataset_id)]

    # Filter by creation time if --older-than specified
    if max_age:
        cutoff = datetime.now(timezone.utc) - max_age
        aged = []
        for ds in matching:
            try:
                dataset = client.get_dataset(ds.reference)
                if dataset.created and dataset.created < cutoff:
                    aged.append(ds)
            except NotFound:
                continue
        matching = aged

    if not matching:
        click.echo("No matching deployments found.")
        return

    click.echo(
        f"\nFound {len(matching)} dataset(s) to delete "
        f"in {target_config.project_id}:\n"
    )
    for ds in sorted(matching, key=lambda d: d.dataset_id):
        click.echo(f"  {ds.dataset_id}")
    click.echo()

    if dry_run:
        click.echo("Dry run — no datasets were deleted.")
        return

    if not yes and not click.confirm(f"Delete {len(matching)} dataset(s)?"):
        click.echo("Aborted.")
        return

    deleted = 0
    for ds in matching:
        dataset_ref = f"{target_config.project_id}.{ds.dataset_id}"
        try:
            client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
            click.echo(f"  Deleted {ds.dataset_id}")
            deleted += 1
        except Exception as e:
            click.echo(f"  Failed to delete {ds.dataset_id}: {e}", err=True)

        # Clean up local files
        local_dir = Path(sql_dir) / target_config.project_id / ds.dataset_id
        if local_dir.exists():
            shutil.rmtree(local_dir)

    click.echo(f"\nDeleted {deleted}/{len(matching)} dataset(s).")
