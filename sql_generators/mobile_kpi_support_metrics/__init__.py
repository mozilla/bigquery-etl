"""Generate active users aggregates per app."""

import click

from bigquery_etl.cli.utils import is_valid_project
from sql_generators.mobile_kpi_support_metrics.mobile_kpi_support_metrics import (
    generate_mobile_kpi_support_metrics,
)


@click.command()
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
@click.option(
    "--target-project",
    "--target_project",
    help="GCP project ID",
    default="moz-fx-data-shared-prod",
    callback=is_valid_project,
)
def generate(target_project, output_dir, **kwargs):
    """Call generate_mobile_kpi_support_metrics function."""
    generate_mobile_kpi_support_metrics(target_project, output_dir)
