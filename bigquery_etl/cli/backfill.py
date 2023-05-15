"""bigquery-etl CLI backfill command."""

import re
import sys
import tempfile
from datetime import date
from pathlib import Path

import click

from ..backfill.parse import DEFAULT_REASON, Backfill, BackfillStatus
from ..backfill.validate import (
    validate_all_entries,
    validate_entries_are_sorted,
    validate_overlap_dates,
)
from ..cli.utils import paths_matching_name_pattern, project_id_option, sql_dir_option

QUALIFIED_TABLE_NAME_RE = re.compile(
    r"([a-zA-z0-9_-]+)\.([a-zA-z0-9_-]+)\.([a-zA-z0-9_-]+)"
)


@click.group(help="Commands for managing backfills.")
@click.pass_context
def backfill(ctx):
    """Create the CLI group for the backfill command."""
    # create temporary directory generated content is written to
    # the directory will be deleted automatically after the command exits
    # TODO:  confirm if this is needed
    ctx.ensure_object(dict)
    ctx.obj["TMP_DIR"] = ctx.with_resource(tempfile.TemporaryDirectory())


@backfill.command(
    help="""Create a new backfill entry in the backfill.yaml file.  Create
    a backfill.yaml file if it does not already exist.

    Examples:

    \b
    ./bqetl backfill create mozdata.telemetry_derived.deviations_v1 \\
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
    default=str(date.today()),
)
@click.option(
    "--exclude",
    "-x",
    multiple=True,
    help="Dates excluded from backfill. Date format: yyyy-mm-dd",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=[],
)
@click.option(
    "--watcher",
    "-w",
    help="Watcher of the backfill (email address)",
    default="example@mozilla.com",
)
@click.pass_context
def create(
    ctx,
    qualified_table_name,
    sql_dir,
    start_date,
    end_date,
    exclude,
    watcher,
):
    """CLI command for creating a new backfill entry."""
    # TODO: repalce with try catch statements:
    if not QUALIFIED_TABLE_NAME_RE.match(qualified_table_name):
        click.echo(
            "Qualified table name must be named like:" + " <project>.<dataset>.<table>"
        )
        sys.exit(1)

    path = Path(sql_dir)

    project_id, dataset_id, table_id = qualified_table_name.split(".")

    query_path = path / project_id / dataset_id / table_id

    if not query_path.exists():
        click.echo(f"{project_id}.{dataset_id}.{table_id}" + " does not exist")
        sys.exit(1)

    backfill_file = query_path / "backfill.yaml"

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

    if backfill_file.exists():
        backfills = Backfill.entries_from_file(backfill_file)
        for entry in backfills:
            if entry.status == BackfillStatus.DRAFTING:
                validate_overlap_dates(backfill, entry)

    backfills.append(backfill)

    sorted_backfills = sorted(backfills, reverse=True)
    validate_entries_are_sorted(sorted_backfills)

    backfill_file.write_text(
        "\n".join(backfill.to_yaml() for backfill in sorted_backfills)
    )

    click.echo(f"Created backfill entry in {backfill_file}")


@backfill.command(
    help="""Validate backfills format and content

    Examples:

    ./bqetl backfill validate mozdata.telemetry_derived.clients_daily_v6

    \b
    # validate all backfill.yaml files if table is not specified
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
    """Validate backfill files."""
    path = Path(sql_dir)

    backfill_files = []

    # TODO: use try statements?
    if qualified_table_name:
        if not QUALIFIED_TABLE_NAME_RE.match(qualified_table_name):
            click.echo(
                "Qualified table name must be named like:"
                + " <project>.<dataset>.<table>"
            )
            sys.exit(1)

        project_id, dataset_id, table_id = qualified_table_name.split(".")
        query_path = path / project_id / dataset_id / table_id

        if not query_path.exists():
            click.echo(f"{project_id}.{dataset_id}.{table_id}" + " does not exist")
            sys.exit(1)

        backfill_file = path / project_id / dataset_id / table_id / "backfill.yaml"

        backfill_files.append(backfill_file)

    else:
        backfill_files = paths_matching_name_pattern(
            None, sql_dir, project_id, ["backfill.yaml"]
        )

    for file in backfill_files:
        if file.exists():
            validate_all_entries(Backfill.entries_from_file(file))
        else:
            click.echo(
                "Backfill.yaml does not exist for :" + " <project>.<dataset>.<table>"
            )
            sys.exit(1)

    # TODO: update to different statements for one file vs all files
    click.echo("All backfill.yaml files have been validated.")
