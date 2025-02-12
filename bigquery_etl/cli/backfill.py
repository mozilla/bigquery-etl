"""bigquery-etl CLI backfill command."""

import json
import logging
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import rich_click as click
import yaml
from dateutil.relativedelta import relativedelta
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
from ..backfill.shredder_mitigation import (
    SHREDDER_MITIGATION_CHECKS_NAME,
    SHREDDER_MITIGATION_QUERY_NAME,
    generate_query_with_shredder_mitigation,
)
from ..backfill.utils import (
    MAX_BACKFILL_ENTRY_AGE_DAYS,
    get_backfill_backup_table_name,
    get_backfill_file_from_qualified_table_name,
    get_backfill_staging_qualified_table_name,
    get_entries_from_qualified_table_name,
    get_qualified_table_name_to_entries_map_by_project,
    get_scheduled_backfills,
    qualified_table_name_matching,
    validate_table_metadata,
)
from ..backfill.validate import (
    validate_depends_on_past_end_date,
    validate_duplicate_entry_with_initiate_status,
    validate_file,
)
from ..cli.query import backfill as query_backfill
from ..cli.utils import (
    billing_project_option,
    is_authenticated,
    project_id_option,
    sql_dir_option,
)
from ..config import ConfigLoader
from ..deploy import FailedDeployException, SkippedDeployException, deploy_table
from ..format_sql.formatter import reformat
from ..metadata.parse_metadata import METADATA_FILE, Metadata, PartitionType
from ..metadata.validate_metadata import SHREDDER_MITIGATION_LABEL
from ..schema import SCHEMA_FILE, Schema

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ignore_missing_metadata_option = click.option(
    "--ignore-missing-metadata",
    is_flag=True,
    default=False,
    help="Ignore backfills for tables missing metadata.yaml. "
    "This can be used to run on checked-in queries without running sql generation.",
)


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
@click.option(
    "--custom_query_path",
    "--custom-query-path",
    help="Path of the custom query to run the backfill. Optional.",
)
@click.option(
    "--shredder_mitigation/--no_shredder_mitigation",
    help="Wether to run a backfill using an auto-generated query that mitigates shredder effect.",
)
@click.option(
    "--override-retention-range-limit",
    "--override_retention_range_limit",
    required=False,
    type=bool,
    is_flag=True,
    help="True to allow running a backfill outside the retention policy limit.",
    default=False,
)
@click.option(
    "--override-depends-on-past-end-date",
    "--override_depends_on_past_end_date",
    is_flag=True,
    help="If set, allow backfill for depends_on_past tables to have an end date before the entry date. "
    "In some cases, this can cause inconsistencies in the data.",
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
    custom_query_path,
    shredder_mitigation,
    override_retention_range_limit,
    override_depends_on_past_end_date,
    billing_project,
):
    """CLI command for creating a new backfill entry in backfill.yaml file.

    A backfill.yaml file will be created if it does not already exist.
    """
    if errors := validate_table_metadata(sql_dir, qualified_table_name):
        click.echo("\n".join(errors))
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
        custom_query_path=custom_query_path,
        shredder_mitigation=shredder_mitigation,
        override_retention_limit=override_retention_range_limit,
        override_depends_on_past_end_date=override_depends_on_past_end_date,
        billing_project=billing_project,
    )

    backfill_file = get_backfill_file_from_qualified_table_name(
        sql_dir, qualified_table_name
    )

    validate_duplicate_entry_with_initiate_status(new_entry, existing_backfills)

    validate_depends_on_past_end_date(new_entry, backfill_file)

    existing_backfills.insert(0, new_entry)

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
@project_id_option()
@ignore_missing_metadata_option
@click.pass_context
def validate(
    ctx,
    qualified_table_name,
    sql_dir,
    project_id,
    ignore_missing_metadata,
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

    errors = defaultdict(list)

    for table_name in backfills_dict:
        try:
            if errors := validate_table_metadata(sql_dir, table_name):
                click.echo("\n".join(errors))
                sys.exit(1)

                try:
                    backfill_file = get_backfill_file_from_qualified_table_name(
                        sql_dir, table_name
                    )
                    validate_file(backfill_file)
                except (yaml.YAMLError, ValueError) as e:
                    errors[table_name].append(
                        f"Backfill.yaml file for {table_name} contains the following error:\n {e}"
                    )
                if table_name in errors:
                    click.echo(f"{BACKFILL_FILE} validation failed for {table_name}")
                else:
                    click.echo(f"{BACKFILL_FILE} has been validated for {table_name}.")
        except FileNotFoundError:
            if ignore_missing_metadata:
                click.echo(f"Skipping {table_name} due to --ignore-missing-metadata")
            else:
                raise
    if len(errors) > 0:
        click.echo("Failed to validate the following backfill entries:")
        for table_name, error_list in errors.items():
            if len(error_list) == 0:
                continue
            click.echo(f"{table_name}:")
            click.echo("\n".join(error_list))
        sys.exit(1)
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
@click.option(
    "--ignore-old-entries",
    is_flag=True,
    default=False,
    help=f"If set, entries older than {MAX_BACKFILL_ENTRY_AGE_DAYS} days will be ignored due "
    "to BigQuery retention settings.",
)
@ignore_missing_metadata_option
@click.pass_context
def scheduled(
    ctx,
    qualified_table_name,
    sql_dir,
    project_id,
    status,
    json_path,
    ignore_old_entries,
    ignore_missing_metadata,
):
    """Return list of backfill(s) that require processing."""
    backfills = get_scheduled_backfills(
        sql_dir,
        project_id,
        qualified_table_name,
        status=status,
        ignore_old_entries=ignore_old_entries,
        ignore_missing_metadata=ignore_missing_metadata,
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

    backfill_staging_qualified_table_name = get_backfill_staging_qualified_table_name(
        qualified_table_name, entry_to_initiate.entry_date
    )

    project, dataset, table = qualified_table_name_matching(qualified_table_name)
    query_path = Path(sql_dir) / project / dataset / table / "query.sql"

    # create schema before deploying staging table if it does not exist
    schema_path = query_path.parent / SCHEMA_FILE

    if not schema_path.exists():
        # if schema doesn't exist, a schema file is created to allow backfill staging table deployment
        Schema.from_query_file(
            query_file=query_path,
            respect_skip=False,
            sql_dir=sql_dir,
        ).to_yaml_file(schema_path)
        click.echo(f"Schema file created for {qualified_table_name}: {schema_path}")

    try:
        deploy_table(
            artifact_file=query_path,
            destination_table=backfill_staging_qualified_table_name,
            respect_dryrun_skip=False,
        )
    except (SkippedDeployException, FailedDeployException) as e:
        raise RuntimeError(
            f"Backfill initiate failed to deploy {query_path} to {backfill_staging_qualified_table_name}."
        ) from e

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

    custom_query_path = None
    checks = None
    custom_checks_name = None

    metadata = Metadata.from_file(
        Path("sql") / project / dataset / table / METADATA_FILE
    )

    # Stop if the metadata contains shredder mitigation label and the backfill doesn't.
    if (
        SHREDDER_MITIGATION_LABEL in metadata.labels
        and entry.shredder_mitigation is False
    ):
        click.echo(
            click.style(
                f"This backfill cannot continue.\nManaged backfills for tables with metadata label"
                f" {SHREDDER_MITIGATION_LABEL} require using --shredder_mitigation.",
                fg="yellow",
            )
        )
        sys.exit(1)

    client = bigquery.Client(project=project)

    if entry.shredder_mitigation is True:
        click.echo(
            click.style(
                f"Generating query with shredder mitigation for {dataset}.{table}...",
                fg="blue",
            )
        )
        query_path, _ = generate_query_with_shredder_mitigation(
            client=client,
            project_id=project,
            dataset=dataset,
            destination_table=table,
            staging_table_name=backfill_staging_qualified_table_name,
            backfill_date=entry.start_date.isoformat(),
        )
        custom_query_path = Path(query_path) / f"{SHREDDER_MITIGATION_QUERY_NAME}.sql"
        checks = True
        custom_checks_name = f"{SHREDDER_MITIGATION_CHECKS_NAME}.sql"
        click.echo(
            click.style(
                f"Starting backfill with custom query: '{custom_query_path}'.",
                fg="blue",
            )
        )
    elif entry.custom_query_path:
        custom_query_path = Path(entry.custom_query_path)

    # rewrite query to query the staging table instead of the prod table if table depends on past
    if metadata.scheduling.get("depends_on_past"):
        query_path = (
            custom_query_path or Path("sql") / project / dataset / table / "query.sql"
        )

        # format to ensure fully qualified references
        query_text = reformat(query_path.read_text())
        updated_query_text = query_text.replace(
            f"`{project}.{dataset}.{table}`",
            f"`{backfill_staging_qualified_table_name}`",
        )

        replaced_ref_query = query_path.parent / "replaced_ref.sql"
        replaced_ref_query.write_text(updated_query_text)

        custom_query_path = replaced_ref_query

    override_retention_limit = entry.override_retention_limit

    # copy previous partition if depends_on_past
    _initialize_previous_partition(
        client,
        qualified_table_name,
        backfill_staging_qualified_table_name,
        metadata,
        entry,
    )

    # Backfill table
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
            **(
                {
                    k: param
                    for k, param in [
                        ("custom_query_path", custom_query_path),
                        ("checks", checks),
                        ("checks_file_name", custom_checks_name),
                    ]
                    if param is not None
                }
            ),
            billing_project=billing_project,
            override_retention_range_limit=override_retention_limit,
        )
    except subprocess.CalledProcessError as e:
        raise ValueError(
            f"Backfill initiate resulted in error for {qualified_table_name}"
        ) from e


def _initialize_previous_partition(
    client: bigquery.Client,
    table_name: str,
    staging_table_name: str,
    metadata: Metadata,
    backfill_entry: Backfill,
):
    """Initialize initial partition for tables with depends_on_past=true.

    For tables with a null date partition parameter, the entire table is copied
    """
    if (
        metadata.scheduling is None
        or not metadata.scheduling.get("depends_on_past")
        or metadata.bigquery is None
        or metadata.bigquery.time_partitioning is None
    ):
        return

    match metadata.bigquery.time_partitioning.type:
        case PartitionType.DAY:
            previous_partition_date = backfill_entry.start_date - timedelta(days=1)
        case PartitionType.MONTH:
            previous_partition_date = backfill_entry.start_date - relativedelta(
                months=1
            )
        case _:
            raise ValueError(
                "Unsupported partitioning type for backfills: "
                f"{metadata.bigquery.time_partitioning.type}"
            )

    partition_param, offset = "submission_date", 0
    if metadata.scheduling:
        partition_param = metadata.scheduling.get(
            "date_partition_parameter", partition_param
        )
        offset = metadata.scheduling.get("date_partition_offset", offset)

    previous_partition_id = get_backfill_partition(
        previous_partition_date,
        partition_param,
        offset,
        metadata.bigquery.time_partitioning.type,
    )

    if previous_partition_id is None:
        raise ValueError(
            f"Unable to get initial partition id for depends_on_past table: {table_name}"
        )

    _copy_table(
        source_table=f"{table_name}${previous_partition_id}",
        destination_table=f"{staging_table_name}${previous_partition_id}",
        client=client,
    )


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
        click.echo(f"Backup table already exists: {destination_table}")
        sys.exit(1)

    click.echo(
        f"Table {source_table} successfully {job_type_str} to {destination_table}"
    )
