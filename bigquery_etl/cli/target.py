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

from bigquery_etl.cli.deploy import deploy as deploy_cmd

from ..cli.utils import sql_dir_option
from ..query_scheduling.formatters import format_timedelta
from ..util.common import block_coding_agents
from ..util.target import (
    IAM_ROLE_MAP,
    MANIFEST_FILENAME,
    _get_git_context,
    _reapply_shared_access,
    extract_commit_from_dataset_name,
    render_artifact_prefix_pattern,
    render_dataset_pattern,
    sanitize_bq_id,
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
    """Parse a duration string like '7d', '24h', '2w', '30m', '30s' into a timedelta."""
    if not value or not re.fullmatch(r"(\d+[wdhms])+", value):
        raise click.BadParameter(
            f"Invalid duration '{value}'. Use format like '7d', '24h', '2w', '30m'."
        )
    return format_timedelta(value)


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

    --email can be repeated to grant access to multiple users in one command.

    Examples:

      ./bqetl --target dev target share --email teammate@mozilla.com

      ./bqetl --target dev target share --email a@mozilla.com --email b@mozilla.com

      ./bqetl --target dev target share --email teammate@mozilla.com --role WRITER

      ./bqetl --target dev target share --branch feature-xyz --email teammate@mozilla.com

      ./bqetl --target dev target share --dataset telemetry_derived --email teammate@mozilla.com

      ./bqetl --target dev target share --table tbl1 --table tbl2 --email teammate@mozilla.com

      ./bqetl --target dev target share --dataset telemetry_derived --table clients_daily_v6 --email teammate@mozilla.com

      ./bqetl --target dev target share --dry-run --email teammate@mozilla.com
    """)
@click.option(
    "--email",
    "emails",
    required=True,
    multiple=True,
    help="Email address of the user to share with (repeatable).",
)
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
    ctx, emails, role, branch, dataset_filters, table_names, dry_run, yes, sql_dir
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

    emails_str = ", ".join(emails)

    if table_names:
        tables_by_ds = _find_matching_tables(
            client, project_id, matching, names=table_names
        )
        if not tables_by_ds:
            click.echo("No matching tables found in target datasets.")
            return

        total = sum(len(tbls) for tbls in tables_by_ds.values())
        click.echo(f"\nWill grant {role} to {emails_str} on {total} table(s):\n")
        for ds_id in sorted(tables_by_ds):
            click.echo(f"  {ds_id}:")
            for tbl in sorted(tables_by_ds[ds_id], key=lambda t: t.table_id):
                click.echo(f"    {tbl.table_id}")
        click.echo()

        if not _confirm(total, "table(s)", dry_run, yes, verb="share"):
            return

        updated = 0
        total_grants = total * len(emails)
        for ds_id, tables in tables_by_ds.items():
            for tbl in tables:
                for email in emails:
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
                        click.echo(
                            f"  {ds_id}.{tbl.table_id}: failed for {email} - {e}",
                            err=True,
                        )
                    _persist_share(
                        sql_dir, project_id, ds_id, tbl.table_id, email, role
                    )
        click.echo(f"\nUpdated {updated}/{total_grants} grant(s).")
    else:
        click.echo(
            f"\nWill grant {role} to {emails_str} on {len(matching)} dataset(s):\n"
        )
        for ds in sorted(matching, key=lambda d: d.dataset_id):
            click.echo(f"  {ds.dataset_id}")
        click.echo()

        if not _confirm(len(matching), "dataset(s)", dry_run, yes, verb="share"):
            return

        _share_datasets(client, matching, emails, role)


@target.command(
    "migrate-branch",
    help="""Migrate a previous branch's target deployment to the current branch.

    Useful after renaming a local git branch: rewrites the sanitized branch
    string in dataset and/or artifact names so existing artifacts match the
    current (new) branch without redeploying from scratch. Must be run
    while checked out on the destination branch.

    Tables are copied to their new location and the originals deleted.
    Views, materialized views, and routines are skipped during the migration;
    the deploy step prompted at the end of the command recreates them at
    the new location from local SQL templates and cleans up the originals.

    Example:

      ./bqetl --target dev target migrate-branch old-feature
    """,
)
@click.argument("old_branch")
@click.option(
    "--dry-run/--no-dry-run",
    "--dry_run/--no_dry_run",
    default=False,
    help="Show the migration plan without executing.",
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
def migrate_branch(ctx, old_branch, dry_run, yes, sql_dir):
    """Migrate a target deployment from old_branch to the current git branch."""
    target_config = ctx.obj["target"]
    project_id = target_config.project_id

    branch_in_dataset = any(
        t and "git.branch" in t
        for t in (target_config.raw_dataset, target_config.raw_dataset_prefix)
    )
    branch_in_artifact = bool(
        target_config.raw_artifact_prefix
        and "git.branch" in target_config.raw_artifact_prefix
    )
    if not (branch_in_dataset or branch_in_artifact):
        raise click.UsageError(
            f"Target '{target_config.name}' has no git.branch in its templates."
        )

    git_context = _get_git_context()
    new_branch = git_context.get("branch", "")
    old_sanitized = sanitize_bq_id(old_branch)
    new_sanitized = sanitize_bq_id(new_branch)
    if old_sanitized == new_sanitized:
        raise click.UsageError(
            f"old_branch '{old_branch}' matches the current branch '{new_branch}'; "
            "nothing to migrate."
        )

    client = bigquery.Client(project=project_id)
    matching = _find_matching_datasets(client, target_config, branch=old_branch)
    if not matching:
        return

    before = len(matching)
    matching, old_commit = _narrow_to_newest_commit(
        client, project_id, target_config, matching, old_branch
    )
    if old_commit and len(matching) < before:
        click.echo(
            f"Multiple commits found for branch; migrating only the most recent: "
            f"{old_commit} ({len(matching)} dataset(s))"
        )

    new_commit = git_context.get("commit", "")
    commit_pair = (
        (old_commit, new_commit) if old_commit and old_commit != new_commit else None
    )

    branch_pair = (old_sanitized, new_sanitized)
    rename_ds_id = _make_transform(
        [branch_pair] if branch_in_dataset else [], commit_pair
    )
    rename_art_id = _make_transform(
        [branch_pair] if branch_in_artifact else [], commit_pair
    )
    # File sweep uses a 4-char guard to avoid rewriting short substrings everywhere.
    rewrite_content = _make_transform(
        [branch_pair] if len(old_sanitized) >= 4 else [], commit_pair
    )

    plan = _build_rename_plan(client, project_id, matching, rename_ds_id, rename_art_id)
    if not plan:
        click.echo("Nothing to migrate.")
        return

    click.echo(f"\nMigration plan for {project_id}:\n")
    for old_ds, new_ds, renames in plan:
        header = f"  {old_ds} -> {new_ds}" if old_ds != new_ds else f"  {old_ds}:"
        click.echo(header)
        for old_t, new_t, ttype in renames:
            click.echo(f"    {old_t} -> {new_t} ({ttype})")
    click.echo()

    if not _confirm(len(plan), "dataset(s)", dry_run, yes, verb="migrate"):
        return

    for old_ds, new_ds, renames in plan:
        _rename_dataset(client, project_id, old_ds, new_ds, renames, sql_dir)

    _rewrite_target_references(sql_dir, project_id, rewrite_content)
    _prompt_and_deploy(ctx, client, project_id, plan, sql_dir, yes)


def _make_transform(branch_pairs, commit_pair):
    """Build a str->str substring-replace transform from (old, new) pairs."""
    replacements = list(branch_pairs)
    if commit_pair:
        replacements.append(commit_pair)

    def transform(s):
        for old, new in replacements:
            s = s.replace(old, new)
        return s

    return transform


def _dataset_modified(client, project_id, ds):
    """Return the dataset's BQ modified timestamp, or None if it cannot be read."""
    try:
        return client.get_dataset(f"{project_id}.{ds.dataset_id}").modified
    except Exception:
        return None


def _narrow_to_newest_commit(client, project_id, target_config, matching, old_branch):
    """Group matching datasets by extracted commit and keep only the newest group.

    Returns (narrowed_matching, old_commit). If the dataset template has no
    git.commit slot (every extraction returns None), returns matching unchanged
    with old_commit=None.
    """
    groups: dict = {}
    for ds in matching:
        commit = extract_commit_from_dataset_name(
            target_config, ds.dataset_id, old_branch
        )
        groups.setdefault(commit, []).append(ds)
    if list(groups) == [None]:
        return matching, None

    epoch = datetime.min.replace(tzinfo=timezone.utc)
    newest = max(
        (c for c in groups if c is not None),
        key=lambda c: max(
            _dataset_modified(client, project_id, ds) or epoch for ds in groups[c]
        ),
    )
    return groups[newest], newest


def _prompt_and_deploy(ctx, client, project_id, plan, sql_dir, yes):
    """Prompt the user to re-deploy renamed artifacts and invoke deploy if confirmed."""
    renamed_paths = [
        str(Path(sql_dir) / project_id / new_ds / new_name)
        for _, new_ds, renames in plan
        for _, new_name, _ in renames
    ]
    if not renamed_paths:
        return
    if not (
        yes
        or click.confirm(
            "\nMigrated tables reflect BigQuery state at migration time, not "
            "local SQL templates. Views, materialized views, and routines were "
            "skipped and don't yet exist at the new location.\n"
            "Run `./bqetl deploy` now to re-render everything from templates? "
            "(skipped views/MVs/routines at the old location will be deleted "
            "first so deploy can recreate them cleanly)",
            default=False,
        )
    ):
        return

    _delete_skipped_artifacts(client, project_id, plan)
    ctx.invoke(
        deploy_cmd,
        paths=tuple(renamed_paths),
        tables=True,
        views=True,
        routines=True,
    )


def _rewrite_target_references(sql_dir, project_id, transform):
    """Apply transform to SQL/YAML files under the target project.

    Catches downstream references (FROM clauses, routine calls, manifests) in
    other artifacts in the target dir that point at renamed names. The caller
    supplies a str->str transform; this function only walks files and writes
    back when the content changed.
    """
    project_dir = Path(sql_dir) / project_id
    if not project_dir.exists():
        return

    updated = 0
    for path in project_dir.rglob("*"):
        if not path.is_file() or path.suffix not in (".sql", ".yaml"):
            continue
        try:
            content = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        new_content = transform(content)
        if new_content == content:
            continue
        path.write_text(new_content)
        updated += 1

    if updated:
        click.echo(f"  Rewrote references in {updated} local file(s)")


def _delete_skipped_artifacts(client, project_id, plan):
    """Delete old views/MVs/routines that rename skipped so deploy can recreate them."""
    for old_ds, new_ds, renames in plan:
        for old_name, _, ttype in renames:
            ref = f"{project_id}.{old_ds}.{old_name}"
            try:
                if ttype == "ROUTINE":
                    client.delete_routine(ref)
                elif ttype in ("VIEW", "MATERIALIZED_VIEW"):
                    client.delete_table(ref, not_found_ok=True)
                else:
                    continue
                click.echo(f"  Deleted {ttype.lower()} {old_ds}.{old_name}")
            except Exception as e:
                click.echo(
                    f"  Failed to delete {ttype.lower()} {old_ds}.{old_name}: {e}",
                    err=True,
                )
        if old_ds != new_ds:
            try:
                client.delete_dataset(f"{project_id}.{old_ds}", not_found_ok=True)
                click.echo(f"  Deleted dataset {old_ds}")
            except Exception as e:
                click.echo(f"  Could not delete dataset {old_ds}: {e}", err=True)


def _build_rename_plan(client, project_id, datasets, rename_ds_id, rename_art_id):
    """Compute [(old_ds, new_ds, [(old_name, new_name, type), ...])] entries.

    Type is the BigQuery table type (TABLE, VIEW, MATERIALIZED_VIEW, ...) or
    "ROUTINE" for UDFs and stored procedures.

    rename_ds_id and rename_art_id are str->str transforms applied to the
    dataset and table/routine names respectively. The caller decides which
    substitutions apply where (branch, commit, both, or none).
    """
    plan = []
    for ds in datasets:
        new_ds_id = rename_ds_id(ds.dataset_id)
        ds_path = f"{project_id}.{ds.dataset_id}"
        try:
            tables = list(client.list_tables(ds_path))
        except NotFound:
            continue
        candidates = [(t.table_id, t.table_type) for t in tables]
        candidates += [(r.routine_id, "ROUTINE") for r in client.list_routines(ds_path)]

        renames = []
        for old_id, ttype in candidates:
            new_id = rename_art_id(old_id)
            if new_ds_id == ds.dataset_id and new_id == old_id:
                continue
            renames.append((old_id, new_id, ttype))
        if new_ds_id != ds.dataset_id or renames:
            plan.append((ds.dataset_id, new_ds_id, renames))
    return plan


def _rename_dataset(client, project_id, old_ds, new_ds, renames, sql_dir):
    """Copy tables in parallel, skip views/MVs/routines (handled by deploy), update local files."""
    if old_ds != new_ds:
        _ensure_new_dataset(client, project_id, old_ds, new_ds)

    jobs = []
    for old_name, new_name, ttype in renames:
        if ttype in ("VIEW", "MATERIALIZED_VIEW", "ROUTINE"):
            click.echo(
                f"  Skipped {ttype.lower()} {old_ds}.{old_name} — deploy will recreate",
                err=True,
            )
            continue
        src = f"{project_id}.{old_ds}.{old_name}"
        dst = f"{project_id}.{new_ds}.{new_name}"
        jobs.append((old_name, new_name, src, client.copy_table(src, dst)))

    for old_t, new_t, src, job in jobs:
        try:
            job.result()
            click.echo(f"  Copied {old_ds}.{old_t} -> {new_ds}.{new_t}")
        except Exception as e:
            click.echo(f"  Failed to copy {old_ds}.{old_t}: {e}", err=True)
            continue

        manifest_file = Path(sql_dir) / project_id / old_ds / old_t / MANIFEST_FILENAME
        _reapply_shared_access(client, f"{project_id}.{new_ds}.{new_t}", manifest_file)

        try:
            client.delete_table(src, not_found_ok=True)
        except Exception as e:
            click.echo(f"  Failed to delete {old_ds}.{old_t}: {e}", err=True)

    _rename_local_files(sql_dir, project_id, old_ds, new_ds, renames)


def _ensure_new_dataset(client, project_id, old_ds, new_ds):
    """Create new_ds (if missing) as a metadata copy of old_ds."""
    new_ds_ref = f"{project_id}.{new_ds}"
    try:
        client.get_dataset(new_ds_ref)
        return
    except NotFound:
        pass
    old = client.get_dataset(f"{project_id}.{old_ds}")
    new = bigquery.Dataset(new_ds_ref)
    new.location = old.location
    new.labels = dict(old.labels) if old.labels else {}
    new.description = old.description
    new.access_entries = list(old.access_entries)
    client.create_dataset(new)
    click.echo(f"  Created dataset {new_ds}")


def _rename_local_files(sql_dir, project_id, old_ds, new_ds, renames):
    """Move local target dirs to match the new BigQuery names."""
    old_dir = Path(sql_dir) / project_id / old_ds
    if not old_dir.exists():
        return
    new_dir = Path(sql_dir) / project_id / new_ds
    new_dir.mkdir(parents=True, exist_ok=True)
    for old_t, new_t, _ in renames:
        src = old_dir / old_t
        dst = new_dir / new_t
        if src == dst or not src.exists() or dst.exists():
            continue
        shutil.move(str(src), str(dst))
    if old_dir != new_dir and old_dir.exists() and not any(old_dir.iterdir()):
        old_dir.rmdir()


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


def _share_datasets(client, datasets, emails, role):
    """Grant dataset-level access to multiple datasets for multiple users."""
    updated = 0
    total_grants = len(datasets) * len(emails)
    for ds in sorted(datasets, key=lambda d: d.dataset_id):
        for email in emails:
            if _grant_dataset_access(client, ds.reference, email, role):
                click.echo(f"  {ds.dataset_id}: granted {role} to {email}")
                updated += 1
            else:
                click.echo(f"  {ds.dataset_id}: {email} already has {role}")

    click.echo(f"\nUpdated {updated}/{total_grants} grant(s).")


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
