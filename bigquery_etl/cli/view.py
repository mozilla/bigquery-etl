"""bigquery-etl CLI view command."""
import click

from bigquery_etl.view import publish_views


@click.group(help="Commands for managing views.")
def view():
    """Create the CLI group for the view command."""
    pass


view.add_command(publish_views.main, "publish")
