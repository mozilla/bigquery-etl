"""Generate metadata and bigconfig files for stable tables."""

import click

from bigquery_etl.cli.utils import is_valid_project
from sql_generators.stable_tables_monitoring.stable_tables_monitoring import (
    generate_stable_table_bigconfig_files,
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
@click.option(
    "--enable-monitoring",
    "--enable_monitoring",
    help="Monitoring enabled true or false",
    default=True,
)
def generate(target_project, output_dir, enable_monitoring, **kwargs):
    """Call generate_stable_table_bigconfig_files function."""
    generate_stable_table_bigconfig_files(target_project, output_dir, enable_monitoring)
