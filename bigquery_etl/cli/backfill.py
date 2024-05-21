"""bigquery-etl CLI backfill command."""

import json
import logging
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import rich_click as click
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound

from ..backfill.date_range import BackfillDateRange, get_backfill_partition
from ..backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_BILLING_PROJECT,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from ..backfill.utils import (
    get_backfill_backup_table_name,
    get_backfill_file_from_qualified_table_name,
    get_backfill_staging_qualified_table_name,
    get_entries_from_qualified_table_name,
    get_qualified_table_name_to_entries_map_by_project,
    get_scheduled_backfills,
    qualified_table_name_matching,
    validate_depends_on_past,
    validate_metadata_workgroups,
)
from ..backfill.validate import (
    validate_duplicate_entry_with_initiate_status,
    validate_file,
)
from ..cli.query import backfill as query_backfill
from ..cli.query import deploy
from ..cli.utils import (
    billing_project_option,
    is_authenticated,
    project_id_option,
    sql_dir_option,
)
from ..config import ConfigLoader
from ..metadata.parse_metadata import METADATA_FILE, Metadata

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.group(help="Commands for managing backfills.")
@click.pass_context
def backfill(ctx):
    """Create the CLI group for the backfill command."""
    # create temporary directory generated content is written to
    # the directory will be deleted automatically after the command exits
    ctx.ensure_object(dict)
    ctx.obj["TMP_DIR"] = ctx.with_resource(tempfile.TemporaryDirectory())


@backfill.command(
    help="""Create a new backfill entry in the backfill.yaml file.  Create
    a backfill.yaml file if it does not already exist.

    Examples:

    \b
    ./bqetl backfill create moz-fx-data-shared-prod.telemetry_derived.deviations_v1 \\
      --start_date=2021-03-01 \\
      --end_date=2021-03-31 \\
      --exclude=2021-03-03 \\
    """,
)
@click.argument("qualified_table_name")
@sql_dir_option
@click.option(
    "--start_date",
    "--start-date",
    "-s",
    help="First date to be backfilled. Date format: yyyy-mm-dd",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
)
@click.option(
    "--end_date",
    "--end-date",
    "-e",
    help="Last date to be backfilled. Date format: yyyy-mm-dd",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.today(),
)
@click.option(
    "--exclude",
    "-x",
    multiple=True,
    help="Dates excluded from backfill. Date format: yyyy-mm-dd",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
)
@click.option(
    "--watcher",
    "-w",
    help="Watcher of the backfill (email address)",
    default=DEFAULT_WATCHER,
)
# If not specified, the billing project will be set to the default billing project when the backfill is initiated.
@billing_project_option()
@click.pass_context
def create(
    ctx,
    qualified_table_name,
    sql_dir,
    start_date,
    end_date,
    exclude,
    watcher,
    billing_project,
):
    """CLI command for creating a new backfill entry in backfill.yaml file.

    A backfill.yaml file will be created if it does not already exist.
    """
    if not validate_depends_on_past(sql_dir, qualified_table_name):
        click.echo("Tables that depend on past are currently not supported.")
        sys.exit(1)

    if not validate_metadata_workgroups(sql_dir, qualified_table_name):
        click.echo("Only mozilla-confidential workgroups are supported.")
        sys.exit(1)

    existing_backfills = get_entries_from_qualified_table_name(
        sql_dir, qualified_table_name
    )

    new_entry = Backfill(
        entry_date=date.today(),
        start_date=start_date.date(),
        end_date=end_date.date(),
        excluded_dates=[e.date() for e in list(exclude)],
        reason=DEFAULT_REASON,
        watchers=[watcher],
        status=BackfillStatus.INITIATE,
        billing_project=billing_project,
    )

    validate_duplicate_entry_with_initiate_status(new_entry, existing_backfills)

    existing_backfills.insert(0, new_entry)

    backfill_file = get_backfill_file_from_qualified_table_name(
        sql_dir, qualified_table_name
    )

    backfill_file.write_text(
        "\n".join(
            backfill.to_yaml() for backfill in sorted(existing_backfills, reverse=True)
        )
    )

    click.echo(f"Created backfill entry in {backfill_file}.")


@backfill.command(
    help="""Validate backfill.yaml file format and content.

    Examples:

    ./bqetl backfill validate moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6

    \b
    # validate all backfill.yaml files if table is not specified
    Use the `--project_id` option to change the project to be validated;
    default is `moz-fx-data-shared-prod`.

    Examples:

    ./bqetl backfill validate
    """
)
@click.argument("qualified_table_name", required=False)
@sql_dir_option
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@click.pass_context
def validate(
    ctx,
    qualified_table_name,
    sql_dir,
    project_id,
):
    """Validate backfill.yaml files."""
    if qualified_table_name:
        backfills_dict = {
            qualified_table_name: get_entries_from_qualified_table_name(
                sql_dir, qualified_table_name
            )
        }
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project_id
        )

    for qualified_table_name in backfills_dict:

        if not validate_depends_on_past(sql_dir, qualified_table_name):
            click.echo(
                f"Tables that depend on past are currently not supported:  {qualified_table_name}"
            )
            sys.exit(1)

        if not validate_metadata_workgroups(sql_dir, qualified_table_name):
            click.echo(
                f"Only mozilla-confidential workgroups are supported.  {qualified_table_name} contain workgroup access that is not supported"
            )
            sys.exit(1)

        try:
            backfill_file = get_backfill_file_from_qualified_table_name(
                sql_dir, qualified_table_name
            )
            validate_file(backfill_file)
        except (yaml.YAMLError, ValueError) as e:
            click.echo(
                f"Backfill.yaml file for {qualified_table_name} contains the following error:\n {e}"
            )
            sys.exit(1)

    if qualified_table_name:
        click.echo(f"{BACKFILL_FILE} has been validated for {qualified_table_name}.")
    elif backfills_dict:
        click.echo(
            f"All {BACKFILL_FILE} files have been validated for project {project_id}."
        )


@backfill.command(
    help="""Get backfill(s) information from all or specific table(s).

    Examples:

    # Get info for specific table.
    ./bqetl backfill info moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6

    \b
    # Get info for all tables.
    ./bqetl backfill info

    \b
    # Get info from all tables with specific status.
    ./bqetl backfill info --status=Initiate
    """,
)
@click.argument("qualified_table_name", required=False)
@sql_dir_option
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@click.option(
    "--status",
    type=click.Choice([s.value for s in BackfillStatus]),
    help="Filter backfills with this status.",
)
@click.pass_context
def info(ctx, qualified_table_name, sql_dir, project_id, status):
    """Return backfill(s) information from all or specific table(s)."""
    if qualified_table_name:
        backfills_dict = {
            qualified_table_name: get_entries_from_qualified_table_name(
                sql_dir, qualified_table_name, status
            )
        }
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project_id, status
        )

    total_backfills_count = 0

    for qualified_table_name, entries in backfills_dict.items():
        entries_count = len(entries)

        total_backfills_count += entries_count

        project, dataset, table = qualified_table_name_matching(qualified_table_name)
        status_str = f" with {status} status" if status is not None else ""
        click.echo(
            f"""{project}.{dataset}.{table} has {entries_count} backfill(s){status_str}:"""
        )

        for entry in entries:
            click.echo(str(entry))

    click.echo(f"\nThere are a total of {total_backfills_count} backfill(s).")


@backfill.command(
    help="""Get information on backfill(s) that require processing.

    Examples:

    # Get info for specific table.
    ./bqetl backfill scheduled moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6

    \b
    # Get info for all tables.
    ./bqetl backfill scheduled
    """,
)
@click.argument("qualified_table_name", required=False)
@sql_dir_option
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@click.option(
    "--status",
    type=click.Choice([s.value for s in BackfillStatus]),
    default=BackfillStatus.INITIATE.value,
    help="Whether to get backfills to process or to complete.",
)
@click.option("--json_path", type=click.Path())
@click.pass_context
def scheduled(ctx, qualified_table_name, sql_dir, project_id, status, json_path=None):
    """Return list of backfill(s) that require processing."""
    backfills = get_scheduled_backfills(
        sql_dir, project_id, qualified_table_name, status=status
    )

    for qualified_table_name, entry in backfills.items():
        click.echo(f"Backfill scheduled for {qualified_table_name}:\n{entry}")

    click.echo(f"{len(backfills)} backfill(s) require processing.")

    if json_path is not None:
        formatted_backfills = [
            {
                "qualified_table_name": qualified_table_name,
                "entry_date": entry.entry_date.strftime("%Y-%m-%d"),
                "watchers": entry.watchers,
            }
            for qualified_table_name, entry in backfills.items()
        ]

        Path(json_path).write_text(json.dumps(formatted_backfills))


@backfill.command(
    help="""Process entry in backfill.yaml with Initiate status that has not yet been processed.

    Examples:

    \b

    # Initiate backfill entry for specific table
    ./bqetl backfill initiate moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6

    Use the `--project_id` option to change the project;
    default project_id is `moz-fx-data-shared-prod`.
    """
)
@click.argument("qualified_table_name")
@click.option(
    "--parallelism",
    default=16,
    type=int,
    help="Maximum number of queries to execute concurrently",
)
@sql_dir_option
@project_id_option(
    ConfigLoader.get("default", "project", fallback="moz-fx-data-shared-prod")
)
@click.pass_context
def initiate(
    ctx,
    qualified_table_name,
    parallelism,
    sql_dir,
    project_id,
):
    """Process backfill entry with initiate status in backfill.yaml file(s)."""
    click.echo("Backfill processing (initiate) started....")

    backfills_to_process_dict = get_scheduled_backfills(
        sql_dir, project_id, qualified_table_name, status=BackfillStatus.INITIATE.value
    )

    if not backfills_to_process_dict:
        click.echo(f"No backfill processed for {qualified_table_name}")
        return

    entry_to_initiate = backfills_to_process_dict[qualified_table_name]

    project, dataset, table = qualified_table_name_matching(qualified_table_name)

    backfill_staging_qualified_table_name = get_backfill_staging_qualified_table_name(
        qualified_table_name, entry_to_initiate.entry_date
    )

    # deploy backfill staging table
    ctx.invoke(
        deploy,
        name=f"{dataset}.{table}",
        project_id=project,
        destination_table=backfill_staging_qualified_table_name,
    )

    billing_project = DEFAULT_BILLING_PROJECT

    # override with billing project from backfill entry
    if entry_to_initiate.billing_project is not None:
        billing_project = entry_to_initiate.billing_project
    elif not billing_project.startswith("moz-fx-data-backfill-"):
        raise ValueError(
            f"Invalid billing project: {billing_project}.  Please use one of the projects assigned to backfills."
        )
        sys.exit(1)

    click.echo(
        f"\nInitiating backfill for {qualified_table_name} with entry date {entry_to_initiate.entry_date} via dry run:"
    )

    _initiate_backfill(
        ctx,
        qualified_table_name,
        backfill_staging_qualified_table_name,
        entry_to_initiate,
        parallelism,
        dry_run=True,
        billing_project=billing_project,
    )

    click.echo(
        f"\nInitiating backfill for {qualified_table_name} with entry date {entry_to_initiate.entry_date}:"
    )
    _initiate_backfill(
        ctx,
        qualified_table_name,
        backfill_staging_qualified_table_name,
        entry_to_initiate,
        parallelism,
        billing_project=billing_project,
    )

    click.echo(
        f"Processed backfill for {qualified_table_name} with entry date {entry_to_initiate.entry_date}"
    )


def _initiate_backfill(
    ctx,
    qualified_table_name: str,
    backfill_staging_qualified_table_name: str,
    entry: Backfill,
    parallelism: int = 16,
    dry_run: bool = False,
    billing_project=DEFAULT_BILLING_PROJECT,
):
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    project, dataset, table = qualified_table_name_matching(qualified_table_name)

    logging_str = f"""Initiating backfill for {qualified_table_name} (destination: {backfill_staging_qualified_table_name}).
                    Query will be executed in {billing_project}."""

    if dry_run:
        logging_str += "  This is a dry run."

    log.info(logging_str)

    # backfill table
    # in the long-run we should remove the query backfill command and require a backfill entry for all backfills
    try:
        ctx.invoke(
            query_backfill,
            name=f"{dataset}.{table}",
            project_id=project,
            start_date=datetime.fromisoformat(entry.start_date.isoformat()),
            end_date=datetime.fromisoformat(entry.end_date.isoformat()),
            exclude=[e.strftime("%Y-%m-%d") for e in entry.excluded_dates],
            destination_table=backfill_staging_qualified_table_name,
            parallelism=parallelism,
            dry_run=dry_run,
            billing_project=billing_project,
        )
    except subprocess.CalledProcessError as e:
        raise ValueError(
            f"Backfill initiate resulted in error for {qualified_table_name}"
        ) from e


@backfill.command(
    help="""Complete entry in backfill.yaml with Complete status that has not yet been processed..

    Examples:

    \b

    # Complete backfill entry for specific table
    ./bqetl backfill complete moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6

    Use the `--project_id` option to change the project;
    default project_id is `moz-fx-data-shared-prod`.
    """
)
@click.argument("qualified_table_name")
@sql_dir_option
@project_id_option("moz-fx-data-shared-prod")
@click.pass_context
def complete(ctx, qualified_table_name, sql_dir, project_id):
    """Process backfill entry with complete status in backfill.yaml file(s)."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login  --update-adc` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    client = bigquery.Client(project=project_id)

    click.echo("Backfill processing (complete) started....")

    backfills_to_process_dict = get_scheduled_backfills(
        sql_dir, project_id, qualified_table_name, status=BackfillStatus.COMPLETE.value
    )

    if not backfills_to_process_dict:
        click.echo(f"No backfill processed for {qualified_table_name}")
        return

    entry_to_complete = backfills_to_process_dict[qualified_table_name]

    click.echo(
        f"Completing backfill for {qualified_table_name} with entry date {entry_to_complete.entry_date}:"
    )

    backfill_staging_qualified_table_name = get_backfill_staging_qualified_table_name(
        qualified_table_name, entry_to_complete.entry_date
    )

    # clone production table
    cloned_table_full_name = get_backfill_backup_table_name(
        qualified_table_name, entry_to_complete.entry_date
    )
    _copy_table(qualified_table_name, cloned_table_full_name, client, clone=True)

    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    table_metadata = Metadata.from_file(
        Path(sql_dir) / project / dataset / table / METADATA_FILE
    )

    _copy_backfill_staging_to_prod(
        backfill_staging_qualified_table_name,
        qualified_table_name,
        client,
        entry_to_complete,
        table_metadata,
    )

    # delete backfill staging table
    client.delete_table(backfill_staging_qualified_table_name)
    click.echo(
        f"Backfill staging table deleted: {backfill_staging_qualified_table_name}"
    )

    click.echo(
        f"Processed backfill for {qualified_table_name} with entry date {entry_to_complete.entry_date}"
    )


def _copy_backfill_staging_to_prod(
    backfill_staging_table: str,
    qualified_table_name: str,
    client: bigquery.Client,
    entry: Backfill,
    table_metadata: Metadata,
):
    """Copy backfill staging table to prod based on table metadata and backfill config.

    If table is
       un-partitioned: copy the entire staging table to production.
       partitioned: determine and copy each partition from staging to production.
    """
    partitioning_type = None
    if table_metadata.bigquery and table_metadata.bigquery.time_partitioning:
        partitioning_type = table_metadata.bigquery.time_partitioning.type

    if partitioning_type is None:
        _copy_table(backfill_staging_table, qualified_table_name, client)
    else:
        backfill_date_range = BackfillDateRange(
            entry.start_date,
            entry.end_date,
            excludes=entry.excluded_dates,
            range_type=partitioning_type,
        )
        # If date_partition_parameter isn't set it's assumed to be submission_date:
        # https://github.com/mozilla/telemetry-airflow/blob/dbc2782fa23a34ae8268e7788f9621089ac71def/utils/gcp.py#L194C48-L194C48
        partition_param, offset = "submission_date", 0
        if table_metadata.scheduling:
            partition_param = table_metadata.scheduling.get(
                "date_partition_parameter", partition_param
            )
            offset = table_metadata.scheduling.get("date_partition_offset", offset)

        for backfill_date in backfill_date_range:
            if (
                partition := get_backfill_partition(
                    backfill_date,
                    partition_param,
                    offset,
                    partitioning_type,
                )
            ) is None:
                raise ValueError(
                    f"Null partition found completing backfill {entry} for {qualified_table_name}."
                )

            production_table = f"{qualified_table_name}${partition}"
            backfill_table = f"{backfill_staging_table}${partition}"
            _copy_table(backfill_table, production_table, client)


def _copy_table(
    source_table: str, destination_table: str, client, clone: bool = False
) -> None:
    """
    Copy and overwrite table from source to destination table.

    If clone is True, clone (previous) production data for backup before swapping stage data into production.
    """
    job_type_str = "copied"

    if clone:
        copy_config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
            operation_type=bigquery.job.copy_.OperationType.CLONE,
            destination_expiration_time=(datetime.now() + timedelta(days=30)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        )
        job_type_str = "cloned"
    else:
        copy_config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            operation_type=bigquery.job.copy_.OperationType.COPY,
        )

    try:
        client.copy_table(
            source_table,
            destination_table,
            job_config=copy_config,
        ).result()
    except NotFound:
        click.echo(f"Source table not found: {source_table}")
        sys.exit(1)
    except Conflict:
        print(f"Backup table already exists: {destination_table}")
        sys.exit(1)

    click.echo(
        f"Table {source_table} successfully {job_type_str} to {destination_table}"
    )
