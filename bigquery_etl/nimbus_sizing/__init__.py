"""
Nimbus pre-launch population sizing ETL.

Fetches Draft experiments from the Experimenter v8 API, executes their
translated BigQuery SQL WHERE clauses against nimbus_targeting_context,
and writes per-experiment eligible-client estimates to GCS and BigQuery.
"""

import json
import logging
from datetime import date, timedelta

import click
import requests
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

EXPERIMENTER_API_URL = (
    "https://experimenter.services.mozilla.com/api/v8/draft-experiments/"
)
NIMBUS_TARGETING_TABLE = "mozdata.firefox_desktop.nimbus_targeting_context"
GCS_BUCKET = "mozanalysis"
GCS_PATH = "population_sizing/drafts_latest.json"
# 10% sample — multiply counts by 10 to estimate full population
SAMPLE_ID_MAX = 10

# All columns available in nimbus_targeting_context, aliased for use in CTEs
_TARGETING_COLUMNS = """
    client_info.client_id,
    metrics.quantity.nimbus_targeting_context_firefox_version AS firefox_version,
    metrics.boolean.nimbus_targeting_context_is_default_browser AS is_default_browser,
    metrics.boolean.nimbus_targeting_context_is_first_startup AS is_first_startup,
    metrics.boolean.nimbus_targeting_context_is_fx_a_enabled AS is_fx_a_enabled,
    metrics.boolean.nimbus_targeting_context_is_fx_a_signed_in AS is_fx_a_signed_in,
    metrics.boolean.nimbus_targeting_context_is_msix AS is_msix,
    metrics.boolean.nimbus_targeting_context_does_app_need_pin AS does_app_need_pin,
    metrics.boolean.nimbus_targeting_context_has_active_enterprise_policies
        AS has_active_enterprise_policies,
    metrics.boolean.nimbus_targeting_context_has_pinned_tabs AS has_pinned_tabs,
    metrics.boolean.nimbus_targeting_context_uses_firefox_sync AS uses_firefox_sync,
    metrics.boolean.nimbus_targeting_context_user_prefers_reduced_motion
        AS user_prefers_reduced_motion,
    metrics.quantity.nimbus_targeting_context_profile_age_created AS profile_age_created,
    metrics.quantity.nimbus_targeting_context_memory_mb AS memory_mb,
    metrics.quantity.nimbus_targeting_context_addresses_saved AS addresses_saved,
    metrics.quantity.nimbus_targeting_context_total_bookmarks_count AS total_bookmarks_count,
    metrics.string.nimbus_targeting_context_locale AS locale,
    metrics.string.nimbus_targeting_context_region AS region,
    metrics.string.nimbus_targeting_context_distribution_id AS distribution_id,
    metrics.object.nimbus_targeting_context_os AS os,
    metrics.object.nimbus_targeting_context_browser_settings AS browser_settings,
    metrics.object.nimbus_targeting_context_attribution_data AS attribution_data,
    metrics.object.nimbus_targeting_context_home_page_settings AS home_page_settings,
    metrics.object.nimbus_targeting_context_addons_info AS addons_info,
    metrics.object.nimbus_targeting_context_user_monthly_activity AS user_monthly_activity,
    metrics.object.nimbus_targeting_environment_pref_values AS pref_values,
    metrics.object.nimbus_targeting_environment_user_set_prefs AS user_set_prefs,
    metrics.object.nimbus_targeting_context_primary_resolution AS primary_resolution
"""


def fetch_experiments(api_url: str) -> list[dict]:
    """Fetch Draft experiments that need a sizing update from Experimenter."""
    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()
    return [
        exp
        for exp in resp.json()
        if exp.get("targetingSql")
        and exp["targetingSql"].get("needsUpdate")
        and exp["targetingSql"].get("sql")
    ]


def _slug_to_col(slug: str) -> str:
    """Convert an experiment slug to a safe BigQuery column alias."""
    return slug.replace("-", "_").replace(".", "_")[:256]


def build_query(experiments: list[dict]) -> str:
    """
    Build a single BigQuery query that counts eligible clients per experiment.

    One COUNT(DISTINCT IF(targeting_sql, client_id, NULL)) column per experiment,
    all in a single table scan.
    """
    today = date.today()
    window_start = today - timedelta(days=7)
    window_end = today - timedelta(days=1)

    cte = f"""\
WITH latest_per_client AS (
  SELECT
    {_TARGETING_COLUMNS.strip()},
    ROW_NUMBER() OVER (
      PARTITION BY client_info.client_id
      ORDER BY submission_timestamp DESC
    ) AS rn
  FROM `{NIMBUS_TARGETING_TABLE}`
  WHERE DATE(submission_timestamp) BETWEEN '{window_start}' AND '{window_end}'
    AND sample_id < {SAMPLE_ID_MAX}
),
clients AS (SELECT * FROM latest_per_client WHERE rn = 1)"""

    cols = [
        f"  COUNT(DISTINCT IF(\n    {exp['targetingSql']['sql']},\n"
        f"    client_id, NULL)) * {SAMPLE_ID_MAX} AS `{_slug_to_col(exp['slug'])}`"
        for exp in experiments
    ]

    return f"{cte}\nSELECT\n" + ",\n".join(cols) + "\nFROM clients"


def run_query(query: str, project: str) -> list[dict]:
    """Execute the BigQuery query and return rows as dicts."""
    client = bigquery.Client(project=project)
    job = client.query(query)
    return [dict(row) for row in job.result()]


def build_results(experiments: list[dict], rows: list[dict]) -> dict:
    """Map flat query result row back to per-experiment result dicts."""
    if not rows:
        return {}

    row = rows[0]
    results = {}
    for exp in experiments:
        slug = exp["slug"]
        col = _slug_to_col(slug)
        eligible_count = row.get(col)
        if eligible_count is None:
            continue
        population_percent = float(exp.get("populationPercent") or 0)
        results[slug] = {
            "eligible_count": int(eligible_count),
            "enrolled_count": round(eligible_count * population_percent / 100),
            "warnings": exp["targetingSql"].get("warnings", []),
        }
    return results


def write_to_gcs(results: dict, bucket_name: str, path: str) -> None:
    """Write results JSON to GCS for Experimenter to read."""
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(path)
    blob.upload_from_string(
        json.dumps({"v1": results}, indent=2),
        content_type="application/json",
    )
    logger.info("Wrote sizing results to gs://%s/%s", bucket_name, path)


def write_to_bigquery(results: dict, project: str, dataset: str, table: str) -> None:
    """Append sizing results to the BigQuery output table."""
    from datetime import datetime, timezone

    client = bigquery.Client(project=project)
    computed_at = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "slug": slug,
            "eligible_count": data["eligible_count"],
            "enrolled_count": data["enrolled_count"],
            "warnings": data["warnings"],
            "computed_at": computed_at,
        }
        for slug, data in results.items()
    ]
    if not rows:
        return
    errors = client.insert_rows_json(f"{project}.{dataset}.{table}", rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    logger.info("Wrote %d rows to %s.%s.%s", len(rows), project, dataset, table)


@click.command()
@click.option("--api-url", default=EXPERIMENTER_API_URL)
@click.option("--project", default="mozdata")
@click.option("--output-dataset", default="experimenter")
@click.option("--output-table", default="nimbus_draft_sizing")
@click.option("--gcs-bucket", default=GCS_BUCKET)
@click.option("--gcs-path", default=GCS_PATH)
@click.option("--dry-run", is_flag=True, help="Print query without running")
def run(api_url, project, output_dataset, output_table, gcs_bucket, gcs_path, dry_run):
    """Run Nimbus pre-launch population sizing ETL."""
    logging.basicConfig(level=logging.INFO)

    experiments = fetch_experiments(api_url)
    logger.info("Found %d experiments needing sizing update", len(experiments))

    if not experiments:
        logger.info("No experiments to process")
        return

    query = build_query(experiments)

    if dry_run:
        click.echo(query)
        return

    rows = run_query(query, project=project)
    results = build_results(experiments, rows)
    logger.info("Got sizing results for %d experiments", len(results))

    write_to_gcs(results, bucket_name=gcs_bucket, path=gcs_path)
    write_to_bigquery(
        results, project=project, dataset=output_dataset, table=output_table
    )
    logger.info("Nimbus sizing ETL complete")
