"""bigquery-etl CLI format command."""

import rich_click as click

from bigquery_etl.cli.utils import parallelism_option
from bigquery_etl.format_sql.format import format as format_sql


@click.command(
    help="""Format SQL files.

    Examples:

    # Format a specific file
    ./bqetl format sql/moz-fx-data-shared-prod/telemetry/core/view.sql

    # Format all SQL files in `sql/`
    ./bqetl format sql

    # Format standard in (will write to standard out)
    echo 'SELECT 1,2,3' | ./bqetl format
    """,
)
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(file_okay=True),
)
@click.option(
    "--check",
    default=False,
    is_flag=True,
    help="do not write changes, just return status;"
    " return code 0 indicates nothing would change;"
    " return code 1 indicates some files would be reformatted",
)
@click.option(
    "--ignore-skip",
    help="Ignore format skip configuration.",
    is_flag=True,
    default=False,
)
@parallelism_option()
def format(paths, check, ignore_skip, parallelism):
    """Apply formatting to SQL files."""
    format_sql(paths, check=check, ignore_skip=ignore_skip, parallelism=parallelism)
