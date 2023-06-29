"""bigquery-etl CLI backfill command."""

import sys
import tempfile
from datetime import date, datetime

import click
import yaml
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from ..backfill.parse import (
    BACKFILL_FILE,
    DEFAULT_REASON,
    DEFAULT_WATCHER,
    Backfill,
    BackfillStatus,
)
from ..backfill.utils import (
    get_backfill_file_from_qualified_table_name,
    get_backfill_staging_qualified_table_name,
    get_qualified_table_name_to_entries_dict,
    get_qualified_table_name_to_entries_map_by_project,
    qualified_table_name_matching,
    validate_metadata_workgroups,
)
from ..backfill.validate import (
    validate_duplicate_entry_dates,
    validate_file,
    validate_overlap_dates,
)
from ..cli.query import backfill as query_backfill
from ..cli.query import deploy
from ..cli.utils import is_authenticated, project_id_option, sql_dir_option


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
@project_id_option("moz-fx-data-shared-prod")
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
@click.pass_context
def create(
    ctx,
    qualified_table_name,
    sql_dir,
    project_id,
    start_date,
    end_date,
    exclude,
    watcher,
):
    """CLI command for creating a new backfill entry in backfill.yaml file.

    A backfill.yaml file will be created if it does not already exist.
    """
    backfills_dict = get_qualified_table_name_to_entries_dict(
        sql_dir, qualified_table_name
    )

    backfill = Backfill(
        entry_date=date.today(),
        start_date=start_date.date(),
        end_date=end_date.date(),
        excluded_dates=[e.date() for e in list(exclude)],
        reason=DEFAULT_REASON,
        watchers=[watcher],
        status=BackfillStatus.DRAFTING,
    )

    backfills = []

    if backfills_dict:
        # There should only be one qualified_table_name with entries
        qualified_table_name = list(backfills_dict.keys())[0]
        entries = backfills_dict[qualified_table_name]

        for entry in entries:
            validate_duplicate_entry_dates(backfill, entry)
            if entry.status == BackfillStatus.DRAFTING:
                validate_overlap_dates(backfill, entry)

        backfills = entries

    backfills.insert(0, backfill)

    backfill_file = get_backfill_file_from_qualified_table_name(
        sql_dir, qualified_table_name
    )

    backfill_file.write_text(
        "\n".join(backfill.to_yaml() for backfill in sorted(backfills, reverse=True))
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
@project_id_option("moz-fx-data-shared-prod")
@click.pass_context
def validate(
    ctx,
    qualified_table_name,
    sql_dir,
    project_id,
):
    """Validate backfill.yaml files."""
    if qualified_table_name:
        backfills_dict = get_qualified_table_name_to_entries_dict(
            sql_dir, qualified_table_name
        )
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project_id
        )

    for qualified_table_name in backfills_dict:
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
    ./bqetl backfill info --status=Drafting
    """,
)
@click.argument("qualified_table_name", required=False)
@sql_dir_option
@project_id_option("moz-fx-data-shared-prod")
@click.option(
    "--status",
    type=click.Choice([s.value.lower() for s in BackfillStatus]),
    help="Filter backfills with this status.",
)
@click.option(
    "--processed",
    type=bool,
    help="Filter backfill entries depending if they have already been processed.  "
    "This flag only applies to backfill entries with drafting status"
    "If not set, all backfills will be returned",
)
@click.pass_context
def info(ctx, qualified_table_name, sql_dir, project_id, status, processed):
    """Return backfill(s) information from all or specific table(s)."""
    if processed is not None:
        if not is_authenticated():
            click.echo(
                "Authentication to GCP required. Run `gcloud auth login` "
                "and check that the project is set correctly."
            )
            sys.exit(1)
    client = bigquery.Client(project=project_id)

    if qualified_table_name:
        backfills_dict = get_qualified_table_name_to_entries_dict(
            sql_dir, qualified_table_name, status
        )
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project_id, status
        )

    total_backfills_count = 0

    for qualified_table_name, entries in backfills_dict.items():
        filtered_entries = []

        for entry in entries:
            if entry.status == BackfillStatus.DRAFTING and processed is not None:
                backfill_staging_qualified_table_name = (
                    get_backfill_staging_qualified_table_name(
                        qualified_table_name, entry.entry_date
                    )
                )

                try:
                    client.get_table(backfill_staging_qualified_table_name)
                    is_entry_processed = True
                except NotFound:
                    is_entry_processed = False

                if processed == is_entry_processed:
                    filtered_entries.append(entry)

            else:
                filtered_entries.append(entry)

        entries_count = len(filtered_entries)

        if entries_count:
            total_backfills_count += entries_count

            project, dataset, table = qualified_table_name_matching(
                qualified_table_name
            )
            status_str = f" with {status} status" if status is not None else ""
            click.echo(
                f"""{project}.{dataset}.{table} has {entries_count} backfill(s){status_str}:"""
            )

            for entry in filtered_entries:
                click.echo(str(entry))

    click.echo(f"\nThere are a total of {total_backfills_count} backfill(s).")


@backfill.command(
    help="""Process backfill.yaml with Drafting status that has not yet been processed.

    Examples:

    \b
    # Process backfill entry for specific table
    ./bqetl backfill process moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6

    \b
    # Process entries from all backfill.yaml files if table is not specified
    ./bqetl backfill process

    Use the `--project_id` option to change the project;
    default project_id is `moz-fx-data-shared-prod`.
    """
)
@click.argument("qualified_table_name", required=False)
@sql_dir_option
@project_id_option("moz-fx-data-shared-prod")
@click.option(
    "--force",
    "--f",
    help="Process the backfill entries even if a staging table already exists",
    default=False,
    is_flag=True,
)
@click.option(
    "--dry_run/--no_dry_run",
    "--dry-run/--no-dry-run",
    help="Dry run the backfill.  Note that staging table(s) will be deployed during dry run",
)
@click.pass_context
def process(ctx, qualified_table_name, sql_dir, project_id, force, dry_run):
    """Process backfill entry with drafting status in backfill.yaml file(s)."""
    click.echo("Backfill processing initiated....")

    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)
    client = bigquery.Client(project=project_id)

    status = BackfillStatus.DRAFTING

    if qualified_table_name:
        backfills_dict = get_qualified_table_name_to_entries_dict(
            sql_dir, qualified_table_name, status.value
        )
    else:
        backfills_dict = get_qualified_table_name_to_entries_map_by_project(
            sql_dir, project_id, status.value
        )

    total_backfills_processed = 0

    for qualified_table_name, entries in backfills_dict.items():
        # do not process backfill if not mozilla-confidential
        if not validate_metadata_workgroups(sql_dir, qualified_table_name):
            click.echo("Only mozilla-confidential workgroups are supported.")
            sys.exit(1)

        if (len(entries)) > 1:
            click.echo(
                f"There should not be more than one entry with status: {status} "
            )
            sys.exit(1)

        if entries:
            entry_to_process = entries[0]

            backfill_staging_qualified_table_name = (
                get_backfill_staging_qualified_table_name(
                    qualified_table_name, entry_to_process.entry_date
                )
            )

            try:
                client.get_table(backfill_staging_qualified_table_name)
                is_entry_processed = True
            except NotFound:
                is_entry_processed = False

            # do not process backfill when staging table already exists
            if not force and is_entry_processed:
                click.echo(
                    f"""
                    The following backfill staging table already exists for {qualified_table_name}:
                    {backfill_staging_qualified_table_name}
                    """
                )
                sys.exit(1)

            project, dataset, table = qualified_table_name_matching(
                qualified_table_name
            )

            # todo: send notification to watcher(s) that backill for file been initiated
            click.echo(f"Processing backfills for {qualified_table_name}:")

            ctx.invoke(
                deploy,
                name=f"{dataset}.{table}",
                project_id=project_id,
                destination_table=backfill_staging_qualified_table_name,
            )

            ctx.invoke(
                query_backfill,
                name=f"{dataset}.{table}",
                project_id=project_id,
                start_date=entry_to_process.start_date,
                end_date=entry_to_process.end_date,
                exclude=entry_to_process.excluded_dates,
                destination_table=backfill_staging_qualified_table_name,
                dry_run=dry_run,
            )

            total_backfills_processed += 1

            # todo: send notification to watcher(s) that backill for file has been completed

    click.echo(
        f"""
               \nA total of {total_backfills_processed} backfill(s) were processed.
               """
    )
