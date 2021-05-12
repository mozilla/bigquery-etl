"""Generate and run baseline_clients_last_seen queries for Glean apps."""

from bigquery_etl.glean_usage import common

TARGET_TABLE_ID = "baseline_clients_last_seen_v1"
PREFIX = "last_seen"
USAGE_TYPES = ("seen", "created_profile", "seen_session_start", "seen_session_end")


def generate(project_id, baseline_table, output_dir=None, output_only=False):
    """Generate the baseline table query."""
    render_kwargs = dict(
        usage_types=USAGE_TYPES,
    )

    common.generate(
        project_id,
        baseline_table,
        PREFIX,
        TARGET_TABLE_ID,
        render_kwargs,
        no_init=False,
        output_dir=output_dir,
        output_only=output_only,
    )
