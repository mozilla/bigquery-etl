"""
Nimbus pre-launch population sizing ETL.

Fetches Draft experiments from the Experimenter v8 API, executes their
translated BigQuery SQL WHERE clauses against nimbus_targeting_context,
and writes per-experiment eligible-client estimates to GCS and BigQuery.
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone

import click
import requests
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

EXPERIMENTER_API_URL = (
    "https://experimenter.services.mozilla.com/api/v8/draft-experiments/"
)
# Use moz-fx-data-shared-prod directly — mozdata hosts views only (repo convention)
NIMBUS_TARGETING_TABLE = (
    "moz-fx-data-shared-prod.firefox_desktop.nimbus_targeting_context"
)
OUTPUT_PROJECT = "moz-fx-data-shared-prod"
GCS_BUCKET = "mozanalysis"
GCS_FOLDER = "population_sizing"
# 10% sample — multiply counts by 10 to estimate full population
SAMPLE_ID_MAX = 10


def fetch_experiments(api_url: str) -> list[dict]:
    """Fetch Draft experiments that need a sizing update from Experimenter."""
    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list):
        raise RuntimeError(
            f"Unexpected response from {api_url}: expected list, got {type(payload).__name__}"
        )
    return [
        exp
        for exp in payload
        if exp.get("targetingSql")
        and exp["targetingSql"].get("needsUpdate")
        and exp["targetingSql"].get("sql")
    ]


def _col_for_index(i: int) -> str:
    """Return a stable, unique BigQuery column alias for experiment index i."""
    return f"exp_{i}"


def build_query(
    experiments: list[dict], submission_date: date, window_days: int = 7
) -> str:
    """
    Build a single BigQuery query that counts eligible clients per experiment.

    Uses indexed column aliases (exp_0, exp_1, …) to avoid slug→column
    collisions. One COUNTIF per experiment in a single table scan.
    clients CTE selects * so injected SQL can reference any ping field.
    """
    window_start = submission_date - timedelta(days=window_days - 1)
    window_end = submission_date

    cte = f"""\
WITH latest_per_client AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY client_info.client_id
      ORDER BY submission_timestamp DESC
    ) AS rn
  FROM `{NIMBUS_TARGETING_TABLE}`
  WHERE DATE(submission_timestamp) BETWEEN '{window_start}' AND '{window_end}'
    AND sample_id < {SAMPLE_ID_MAX}
    AND client_info.client_id IS NOT NULL
),
clients AS (SELECT * EXCEPT (rn) FROM latest_per_client WHERE rn = 1)"""

    cols = [
        f"  COUNTIF(\n    {exp['targetingSql']['sql']}\n"
        f"  ) * {SAMPLE_ID_MAX} AS `{_col_for_index(i)}`"
        for i, exp in enumerate(experiments)
    ]

    return f"{cte}\nSELECT\n" + ",\n".join(cols) + "\nFROM clients"


def _dry_run_sql(sql: str, project: str) -> bool:
    """Return True if sql is a valid BigQuery expression, False otherwise."""
    # Wrap in a minimal query so BQ can validate the expression syntax
    probe = (
        f"SELECT COUNTIF({sql}) AS c "
        f"FROM `{NIMBUS_TARGETING_TABLE}` WHERE FALSE"
    )
    try:
        client = bigquery.Client(project=project)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        client.query(probe, job_config=job_config)
        return True
    except Exception as exc:
        logger.warning("SQL dry-run failed: %s", exc)
        return False


def run_query(query: str, project: str) -> list[dict]:
    """Execute the BigQuery query and return rows as dicts."""
    client = bigquery.Client(project=project)
    job = client.query(query)
    return [dict(row) for row in job.result()]


def build_results(
    experiments: list[dict], rows: list[dict]
) -> dict:
    """Map flat query result row back to per-experiment result dicts.

    Uses positional index aliases (exp_0, exp_1, …) to avoid slug collisions.
    """
    if not rows:
        return {}

    row = rows[0]
    results = {}
    for i, exp in enumerate(experiments):
        col = _col_for_index(i)
        eligible_count = row.get(col)
        if eligible_count is None:
            continue
        results[exp["slug"]] = {
            "eligible_count": int(eligible_count),
            "warnings": exp["targetingSql"].get("warnings", []),
        }
    return results


def write_to_gcs(
    results: dict, bucket_name: str, folder: str, run_date: str
) -> None:
    """Write versioned sizing results to GCS for Experimenter to read.

    Merges new results into the existing latest file so experiments that
    flipped needsUpdate=false are preserved until they disappear from the API.

    Writes two files (matching the enrollment_funnel pattern):
      - nimbus_draft_sizing_v1_{date}.json  — dated archive
      - nimbus_draft_sizing_v1_latest.json  — consumed by Experimenter
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Merge: read existing latest, update with new results, drop gone slugs
    latest_path = f"{folder}/nimbus_draft_sizing_v1_latest.json"
    existing = {}
    blob = bucket.blob(latest_path)
    if blob.exists():
        try:
            existing = json.loads(blob.download_as_text()).get("v1", {})
        except Exception:
            pass  # treat unreadable existing file as empty

    merged = {**existing, **results}
    json_str = json.dumps({"v1": merged}, indent=2)

    dated_path = f"{folder}/nimbus_draft_sizing_v1_{run_date}.json"
    bucket.blob(dated_path).upload_from_string(
        json_str, content_type="application/json"
    )
    logger.info("Wrote sizing results to gs://%s/%s", bucket_name, dated_path)

    bucket.copy_blob(bucket.blob(dated_path), bucket, latest_path)
    logger.info("Copied to gs://%s/%s", bucket_name, latest_path)


def write_to_bigquery(
    results: dict,
    project: str,
    dataset: str,
    table: str,
    submission_date: date,
) -> None:
    """Write sizing results to BigQuery using WRITE_TRUNCATE on the date partition.

    Truncates the day partition before writing so retries are idempotent.
    """
    client = bigquery.Client(project=project)
    computed_at = datetime.combine(submission_date, datetime.min.time()).replace(
        tzinfo=timezone.utc
    )
    rows = [
        {
            "slug": slug,
            "eligible_count": data["eligible_count"],
            "warnings": data["warnings"],
            "computed_at": computed_at.isoformat(),
        }
        for slug, data in results.items()
    ]
    if not rows:
        return

    partition_decorator = submission_date.strftime("%Y%m%d")
    table_ref = f"{project}.{dataset}.{table}${partition_decorator}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("slug", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("eligible_count", "INTEGER"),
            bigquery.SchemaField("warnings", "STRING", mode="REPEATED"),
            bigquery.SchemaField("computed_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    logger.info("Wrote %d rows to %s", len(rows), table_ref)


@click.command()
@click.option("--date", "submission_date", required=True, help="Execution date (YYYY-MM-DD)")
@click.option("--api-url", default=EXPERIMENTER_API_URL)
@click.option("--project", default=OUTPUT_PROJECT, help="BQ project for query billing and output")
@click.option("--output-dataset", default="experimenter")
@click.option("--output-table", default="nimbus_draft_sizing_v1")
@click.option("--gcs-bucket", default=GCS_BUCKET)
@click.option("--gcs-folder", default=GCS_FOLDER)
@click.option("--dry-run", is_flag=True, help="Print query without running")
def run(
    submission_date,
    api_url,
    project,
    output_dataset,
    output_table,
    gcs_bucket,
    gcs_folder,
    dry_run,
):
    """Run Nimbus pre-launch population sizing ETL."""
    logging.basicConfig(level=logging.INFO)
    run_date = date.fromisoformat(submission_date)

    experiments = fetch_experiments(api_url)
    logger.info("Found %d experiments needing sizing update", len(experiments))

    if not experiments:
        logger.info("No experiments to process")
        return

    # Dry-run each SQL fragment individually — drop bad ones so one bad
    # translation doesn't block the rest of the experiments
    valid_experiments = []
    for exp in experiments:
        sql = exp["targetingSql"]["sql"]
        if _dry_run_sql(sql, project=project):
            valid_experiments.append(exp)
        else:
            logger.warning(
                "Skipping %s — SQL failed dry-run validation", exp["slug"]
            )

    if not valid_experiments:
        logger.info("No valid experiments after dry-run validation")
        return

    query = build_query(valid_experiments, submission_date=run_date)

    if dry_run:
        click.echo(query)
        return

    rows = run_query(query, project=project)
    results = build_results(valid_experiments, rows)
    logger.info("Got sizing results for %d experiments", len(results))

    write_to_gcs(results, bucket_name=gcs_bucket, folder=gcs_folder, run_date=submission_date)
    write_to_bigquery(
        results,
        project=project,
        dataset=output_dataset,
        table=output_table,
        submission_date=run_date,
    )
    logger.info("Nimbus sizing ETL complete")
