"""bigquery-etl CLI backfill command."""

import re
import sys
import tempfile
from collections import OrderedDict
from datetime import date
from pathlib import Path

import cattrs
import click
import yaml

from ..backfill import validate_backfill
from ..backfill.parse_backfill import Backfill
from ..cli.utils import sql_dir_option

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
# TODO: allow excluded dates to be list of type dates or strings (range: start_date..end_date)
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

    entry_date = date.today()

    backfill = Backfill(
        entry_date=entry_date,
        start_date=start_date.date(),
        end_date=end_date.date(),
        excluded_dates=[e.date() for e in list(exclude)],
        reason="Please provide a reason for the backfill and links to any related bugzilla or jira tickets",
        watchers=[watcher],
        status="Drafting",
    )

    backfills = OrderedDict()

    if backfill_file.exists():
        backfills = Backfill.from_backfill_file(backfill_file)
        validate_backfill.validate(backfill, backfills)

    backfills[entry_date] = backfill
    backfills.move_to_end(entry_date, last=False)

    converter = cattrs.BaseConverter()

    for entry_date, entry in backfills.items():
        entry = converter.unstructure(entry)
        entry = Backfill.clean(entry)
        backfills[entry_date] = entry

    validate_backfill.validate_entries(backfills)

    backfill_file.write_text(
        yaml.dump(
            converter.unstructure(backfills),
            default_flow_style=False,
            sort_keys=False,
        )
    )

    click.echo(f"Created backfill entry in {backfill_file}")


# TODO: call validate backfill from circle ci
#  no tables passed in then validate all,
#  if table passed in then validate only one table
# @click.pass_context
# def validate(ctx,
#            qualified_table_name,
# ):
