"""bigquery-etl CLI events_daily command."""
import click

from ..cli.utils import is_valid_project
from ..events_daily.generate_queries import generate_queries


@click.group(
    help="Commands for generating the events daily tables. " "(event_types, etc.)"
)
def events_daily():
    """Create the CLI group for the events_daily command."""
    pass


@events_daily.command()
@click.option(
    "--project-id",
    "--project_id",
    help="GCP project ID",
    default="moz-fx-data-shared-prod",
    callback=is_valid_project,
)
@click.option(
    "--path",
    help="Where query directories will be searched for",
    default="bigquery_etl/events_daily/query_templates",
)
@click.option(
    "--dataset",
    help="The dataset to run this for. "
    "If none selected, runs on all in the configuration yaml file.",
    default=None,
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
def generate(project_id, path, dataset, output_dir):
    """Generate Query directories."""
    generate_queries(project_id, path, dataset, output_dir)
