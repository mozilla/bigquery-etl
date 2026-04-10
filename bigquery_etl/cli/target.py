"""Commands for managing target environments."""

import logging
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..cli.utils import sql_dir_option
from ..query_scheduling.formatters import format_timedelta
from ..util.common import block_coding_agents
from ..util.target import (
    IAM_ROLE_MAP,
    MANIFEST_FILENAME,
    render_artifact_prefix_pattern,
    render_dataset_pattern,
)

log = logging.getLogger(__name__)


def _find_matching_datasets(client, target_config, branch=None):
    """Find datasets in the target project matching the target's naming pattern.

    Returns a list of dataset references, or an empty list (with user message).
    """
    dataset_re = re.compile(render_dataset_pattern(target_config, branch=branch))
    all_datasets = list(client.list_datasets())

    if not all_datasets:
        click.echo(f"No datasets found in project {target_config.project_id}.")
        return []

    matching = [ds for ds in all_datasets if dataset_re.match(ds.dataset_id)]
    if not matching:
        click.echo("No matching datasets found.")

    return matching


def _parse_duration(value: str) -> timedelta:
    """Parse a duration string like '7d', '24h', '2w', '30m' into a timedelta."""
    result = format_timedelta(value)
    if not isinstance(result, timedelta):
        raise click.BadParameter(
            f"Invalid duration '{value}'. Use format like '7d', '24h', '2w', '30m'."
        )
    return result


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

    client = bigquery.Client(project=project_id)
    matching = _find_matching_datasets(client, target_config, branch)
    if not matching:
        return

    # build table-level filters
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
    else:
        # Dataset-level cleanup: --branch (when branch is only in dataset) or --all
        click.echo(f"\nFound {len(matching)} dataset(s) to delete in {project_id}:\n")
        for ds in sorted(matching, key=lambda d: d.dataset_id):
            click.echo(f"  {ds.dataset_id}")
        click.echo()

        if not _confirm(len(matching), "dataset(s)", dry_run, yes):
            return

        deleted = _delete_datasets(client, project_id, matching, sql_dir)
        click.echo(f"\nDeleted {deleted}/{len(matching)} dataset(s).")


@target.command(help="""Share target artifacts with a teammate.

    Discovers datasets via the target's naming pattern (same as `clean`) and
    grants access. Use --branch to narrow to a specific branch.

    Without --table, grants dataset-level access. With --table, grants
    table-level IAM access to specific tables within matched datasets.
    Table-level sharing is persisted in the local manifest so it is re-applied
    automatically when artifacts are re-deployed.

    --dataset and --table filter the template-discovered results and can be
    repeated. They can also be combined: --dataset narrows datasets, --table
    selects tables within those datasets.

    Examples:

      ./bqetl --target dev target share --email teammate@mozilla.com

      ./bqetl --target dev target share --email teammate@mozilla.com --role WRITER

      ./bqetl --target dev target share --branch feature-xyz --email teammate@mozilla.com

      ./bqetl --target dev target share --dataset telemetry_derived --email teammate@mozilla.com

      ./bqetl --target dev target share --table tbl1 --table tbl2 --email teammate@mozilla.com

      ./bqetl --target dev target share --dataset telemetry_derived --table clients_daily_v6 --email teammate@mozilla.com

      ./bqetl --target dev target share --dry-run --email teammate@mozilla.com
    """)
@click.option("--email", required=True, help="Email address of the user to share with.")
@click.option(
    "--role",
    type=click.Choice(["READER", "WRITER", "OWNER"], case_sensitive=False),
    default="READER",
    help="Access role to grant (default: READER).",
)
@click.option(
    "--branch", default=None, help="Share only datasets for a specific branch."
)
@click.option(
    "--dataset",
    "dataset_filters",
    multiple=True,
    help="Narrow to datasets whose name contains this string (repeatable).",
)
@click.option(
    "--table",
    "table_names",
    multiple=True,
    help="Share specific tables within matched datasets (repeatable).",
)
@click.option(
    "--dry-run/--no-dry-run",
    "--dry_run/--no_dry_run",
    default=False,
    help="Show what would be shared without actually sharing.",
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
def share(
    ctx, email, role, branch, dataset_filters, table_names, dry_run, yes, sql_dir
):
    """Share target artifacts with a teammate."""
    target_config = ctx.obj["target"]
    project_id = target_config.project_id
    client = bigquery.Client(project=project_id)

    matching = _find_matching_datasets(client, target_config, branch)
    if not matching:
        return

    if dataset_filters:
        matching = [
            ds for ds in matching if any(f in ds.dataset_id for f in dataset_filters)
        ]
        if not matching:
            click.echo("No datasets match the given --dataset filter(s).")
            return

    if table_names:
        tables_by_ds = _find_matching_tables(
            client, project_id, matching, names=table_names
        )
        if not tables_by_ds:
            click.echo("No matching tables found in target datasets.")
            return

        total = sum(len(tbls) for tbls in tables_by_ds.values())
        click.echo(f"\nWill grant {role} to {email} on {total} table(s):\n")
        for ds_id in sorted(tables_by_ds):
            click.echo(f"  {ds_id}:")
            for tbl in sorted(tables_by_ds[ds_id], key=lambda t: t.table_id):
                click.echo(f"    {tbl.table_id}")
        click.echo()

        if not _confirm(total, "table(s)", dry_run, yes, verb="share"):
            return

        updated = 0
        for ds_id, tables in tables_by_ds.items():
            for tbl in tables:
                try:
                    if _grant_table_access(client, tbl.reference, email, role):
                        click.echo(
                            f"  {ds_id}.{tbl.table_id}: granted {role} to {email}"
                        )
                        updated += 1
                    else:
                        click.echo(
                            f"  {ds_id}.{tbl.table_id}: {email} already has {role}"
                        )
                except Exception as e:
                    click.echo(f"  {ds_id}.{tbl.table_id}: failed - {e}", err=True)
                _persist_share(sql_dir, project_id, ds_id, tbl.table_id, email, role)
        click.echo(f"\nUpdated {updated}/{total} table(s).")
    else:
        click.echo(f"\nWill grant {role} to {email} on {len(matching)} dataset(s):\n")
        for ds in sorted(matching, key=lambda d: d.dataset_id):
            click.echo(f"  {ds.dataset_id}")
        click.echo()

        if not _confirm(len(matching), "dataset(s)", dry_run, yes, verb="share"):
            return

        _share_datasets(client, matching, email, role)


def _grant_dataset_access(client, dataset_ref, email, role):
    """Grant dataset-level access. Returns True if updated, False if already granted.

    Raises NotFound if dataset doesn't exist.
    """
    dataset = client.get_dataset(dataset_ref)
    entries = list(dataset.access_entries)

    if any(e.entity_id == email and e.role == role for e in entries):
        return False

    entries.append(
        bigquery.AccessEntry(role=role, entity_type="userByEmail", entity_id=email)
    )
    dataset.access_entries = entries
    client.update_dataset(dataset, ["access_entries"])
    return True


def _grant_table_access(client, table_ref, email, role):
    """Grant table-level IAM access. Returns True if updated, False if already granted.

    Raises NotFound if table doesn't exist.
    """
    table = client.get_table(table_ref)
    iam_role = IAM_ROLE_MAP[role]
    member = f"user:{email}"

    policy = client.get_iam_policy(table)
    if any(
        b["role"] == iam_role and member in b.get("members", set())
        for b in policy.bindings
    ):
        return False

    policy.bindings.append({"role": iam_role, "members": {member}})
    client.set_iam_policy(table, policy)
    return True


def _share_datasets(client, datasets, email, role):
    """Grant dataset-level access to multiple datasets."""
    updated = 0
    for ds in sorted(datasets, key=lambda d: d.dataset_id):
        if _grant_dataset_access(client, ds.reference, email, role):
            click.echo(f"  {ds.dataset_id}: granted {role} to {email}")
            updated += 1
        else:
            click.echo(f"  {ds.dataset_id}: {email} already has {role}")

    click.echo(f"\nUpdated {updated}/{len(datasets)} dataset(s).")


def _persist_share(sql_dir, project_id, dataset_id, table_id, email, role):
    """Persist table sharing to manifest so it survives re-deploys."""
    manifest_path = (
        Path(sql_dir) / project_id / dataset_id / table_id / MANIFEST_FILENAME
    )
    if not manifest_path.exists():
        return
    manifest = yaml.safe_load(manifest_path.read_text()) or {}
    shared_with = manifest.get("shared_with", [])
    if not any(s["email"] == email and s["role"] == role for s in shared_with):
        shared_with.append({"email": email, "role": role})
        manifest["shared_with"] = shared_with
        manifest_path.write_text(yaml.dump(manifest))


def _find_matching_tables(
    client, project_id, datasets, artifact_re=None, cutoff=None, names=None
):
    """Find tables within datasets that match the given filters.

    Returns a dict of dataset_id -> list of table references.
    """
    name_set = set(names) if names else None
    result = {}
    for ds in datasets:
        try:
            tables = list(client.list_tables(f"{project_id}.{ds.dataset_id}"))
        except NotFound:
            continue
        matched = []
        for tbl in tables:
            if name_set and not any(n in tbl.table_id for n in name_set):
                continue
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
                local_ds_dir = Path(sql_dir) / project_id / dataset_id
                if local_ds_dir.exists():
                    shutil.rmtree(local_ds_dir)
            except Exception as e:
                click.echo(
                    f"  Failed to remove empty dataset {dataset_id}: {e}",
                    err=True,
                )

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


def _confirm(count, label, dry_run, yes, verb="delete"):
    """Prompt for confirmation. Returns True to proceed."""
    if dry_run:
        click.echo(f"Dry run: would {verb} {count} {label}.")
        return False
    if not yes and not click.confirm(f"{verb.title()} {count} {label}?"):
        click.echo("Aborted.")
        return False
    return True
