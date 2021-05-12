"""Generate and run baseline_clients_first_seen queries for Glean apps."""

from bigquery_etl.glean_usage import common

TARGET_TABLE_ID = "baseline_clients_first_seen_v1"
PREFIX = "first_seen"


def generate(project_id, baseline_table, output_dir=None, output_only=False):
    """Generate the baseline table query."""

    render_kwargs = dict(
        # do not match on org_mozilla_firefoxreality
        fennec_id=any(
            (f"{app_id}_stable" in baseline_table)
            for app_id in [
                "org_mozilla_firefox",
                "org_mozilla_fenix_nightly",
                "org_mozilla_fennec_aurora",
                "org_mozilla_firefox_beta",
                "org_mozilla_fenix",
            ]
        ),
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
