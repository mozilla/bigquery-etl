"""bigquery-etl CLI format command."""

import click

from bigquery_etl.format_sql.format import format as format_sql


@click.command(
    help="Format SQL.",
)
@click.argument(
    "path",
    default="moz-fx-data-shared-prod/sql/",
    type=click.Path(file_okay=True),
)
def format(path):
    """Apply formatting to SQL files."""
    format_sql([path])
