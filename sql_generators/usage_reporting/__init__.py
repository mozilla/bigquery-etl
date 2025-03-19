"""Usage Reporting ETL."""

import click

from bigquery_etl.cli.utils import is_valid_project, use_cloud_function_option
from sql_generators.usage_reporting.usage_reporting import generate_usage_reporting

COMPOSITE_ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE = (
    "composite_active_users_aggregates.view.sql.jinja"
)


@click.command()
@click.option(
    "--target-project",
    "--target_project",
    help="GCP project ID",
    default="moz-fx-data-shared-prod",
    callback=is_valid_project,
)
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
@click.option(
    "--parallelism",
    "-p",
    help="Maximum number of tasks to execute concurrently",
    default=8,
)
@click.option(
    "--except",
    "-x",
    "exclude",
    help="Process all tables except for the given tables",
)
@click.option(
    "--only",
    "-o",
    help="Process only the given tables",
)
@click.option(
    "--app_name",
    "--app-name",
    help="App to generate per-app dataset metadata and union views for.",
)
@use_cloud_function_option
def generate(target_project, output_dir, **kwargs):
    """Call generate_usage_reporting function."""
    generate_usage_reporting(target_project, output_dir)
