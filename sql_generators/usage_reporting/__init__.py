"""Usage Reporting ETL."""

from os import path
from pathlib import Path

import click

from bigquery_etl.cli.utils import is_valid_project, use_cloud_function_option
from sql_generators.usage_reporting.usage_reporting import generate_usage_reporting

GENERATOR_ROOT = Path(path.dirname(__file__))

HEADER = f"Generated via `{GENERATOR_ROOT.name}` SQL generator."
VERSION = "v1"

TEMPLATES_LOCATION = "templates"

CHANNEL_TEMPLATES = (
    "usage_reporting_clients_daily_v1.query.sql.jinja",
    "usage_reporting_clients_first_seen_v1.query.sql.jinja",
    "usage_reporting_clients_last_seen_v1.query.sql.jinja",
)
CHANNEL_VIEW_TEMPLATE = "channel.view.sql.jinja"

ARTIFACT_TEMPLATES = (
    "metadata.yaml.jinja",
    "schema.yaml.jinja",
)
APP_UNION_VIEW_TEMPLATE = "app_union.view.sql.jinja"

ACTIVE_USERS_VIEW_TEMPLATE = "usage_reporting_active_users.view.sql.jinja"
COMPOSITE_ACTIVE_USERS_TEMPLATE = "composite_active_users.view.sql.jinja"

ACTIVE_USERS_AGGREGATES_TEMPLATE = (
    "usage_reporting_active_users_aggregates_v1.query.sql.jinja"
)
ACTIVE_USERS_AGGREGATES_VIEW_TEMPLATE = (
    "usage_reporting_active_users_aggregates.view.sql.jinja"
)

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
