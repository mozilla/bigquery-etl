"""Generate metadata and bigconfig files for stable tables."""

import click

from bigquery_etl.cli.utils import is_valid_project
from sql_generators.stable_tables_monitoring.stable_tables_monitoring import (
    generate_stable_table_bigconfig_files,
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
    "--enable-monitoring",
    "--enable_monitoring",
    help="Monitoring enabled true or false",
    default=True,
)
def generate(target_project, enable_monitoring, **kwargs):
    """Call generate_mobile_kpi_support_metrics function."""
    generate_stable_table_bigconfig_files(target_project, enable_monitoring)
