"""Generating and run baseline_clients_daily queries for Glean apps."""
from bigquery_etl.glean_usage import common

BASELINE_DAILY_TABLE_ID = "baseline_clients_daily_v1"
PREFIX = "daily"


def generate(project_id, baseline_table, output_dir=None, output_only=False):
    """Generate the baseline table query."""
    common.generate(
        project_id,
        baseline_table,
        PREFIX,
        BASELINE_DAILY_TABLE_ID,
        {},
        no_init=False,
        output_dir=output_dir,
        output_only=output_only,
    )
