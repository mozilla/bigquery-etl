#!/usr/bin/env python3

"""Nimbus pre-launch population sizing ETL.

Fetches Draft experiments from the Experimenter v8 API, executes their
translated BigQuery SQL WHERE clauses against nimbus_targeting_context,
and writes per-experiment eligible-client estimates to GCS and BigQuery.

Supports Desktop (firefox_desktop), Fenix (fenix), and iOS (firefox_ios).
Deduplicates on client_info.client_id (desktop) or client_id (mobile).
profile_group_id support requires a separate ticket to add that field to
nimbus_targeting_context first.
"""

import json
import logging
from argparse import ArgumentParser
from datetime import date, datetime, timedelta, timezone

import requests
from google.cloud import bigquery, storage  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXPERIMENTER_API_URL = (
    "https://experimenter.services.mozilla.com/api/v8/draft-experiments/"
)
NIMBUS_TARGETING_TABLE = (
    "moz-fx-data-shared-prod.firefox_desktop.nimbus_targeting_context"
)
NIMBUS_TARGETING_TABLE_FENIX = (
    "moz-fx-data-shared-prod.fenix_derived.nimbus_recorded_targeting_context_v1"
)
NIMBUS_TARGETING_TABLE_IOS = (
    "moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived"
    ".nimbus_recorded_targeting_context_v1"
)
APP_TARGETING_TABLES = {
    "firefox_desktop": NIMBUS_TARGETING_TABLE,
    "fenix": NIMBUS_TARGETING_TABLE_FENIX,
    "firefox_ios": NIMBUS_TARGETING_TABLE_IOS,
}
_MOBILE_APPS = frozenset({"fenix", "firefox_ios"})
GCS_BUCKET = "mozanalysis"
GCS_FOLDER = "population_sizing"
# 10% sample -- multiply counts by 10 to estimate full population
SAMPLE_ID_MAX = 10


parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
parser.add_argument("--project", default="moz-fx-data-experiments")
parser.add_argument("--output-dataset", default="monitoring")
parser.add_argument("--output-table", default="experiment_population_estimates_v1")
parser.add_argument("--api-url", default=EXPERIMENTER_API_URL)
parser.add_argument("--gcs-bucket", default=GCS_BUCKET)
parser.add_argument("--gcs-folder", default=GCS_FOLDER)
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print query without running",
)


def fetch_experiments(api_url: str) -> list:
    """Fetch Draft experiments that need a sizing update from Experimenter."""
    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list):
        raise RuntimeError(
            f"Unexpected response from {api_url}: "
            f"expected list, got {type(payload).__name__}"
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
    experiments: list,
    submission_date: date,
    table: str = NIMBUS_TARGETING_TABLE,
    is_mobile: bool = False,
    window_days: int = 7,
) -> str:
    """Build a single BigQuery query counting eligible clients per experiment.

    Uses indexed column aliases (exp_0, exp_1, ...) to avoid slug-to-column
    collisions. One COUNTIF per experiment in a single table scan.
    clients CTE selects * so injected SQL can reference any ping field.
    Desktop deduplicates on client_info.client_id with sample_id < 10.
    Mobile deduplicates on client_id with hash-based 10% sampling (no
    sample_id column in mobile derived tables).
    """
    window_start = submission_date - timedelta(days=window_days - 1)
    window_end = submission_date

    if is_mobile:
        cte = f"""\
WITH latest_per_client AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY client_id
      ORDER BY submission_date DESC
    ) AS rn
  FROM `{table}`
  WHERE submission_date BETWEEN '{window_start}' AND '{window_end}'
    AND ABS(MOD(FARM_FINGERPRINT(client_id), 10)) < 1
    AND client_id IS NOT NULL
),
clients AS (SELECT * EXCEPT (rn) FROM latest_per_client WHERE rn = 1)"""
    else:
        cte = f"""\
WITH latest_per_client AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY client_info.client_id
      ORDER BY submission_timestamp DESC
    ) AS rn
  FROM `{table}`
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


def _dry_run_sql(
    sql: str, project: str, table: str = NIMBUS_TARGETING_TABLE
) -> bool:
    """Return True if sql is a valid BigQuery expression, False otherwise."""
    probe = f"SELECT COUNTIF({sql}) AS c FROM `{table}` WHERE FALSE"
    try:
        client = bigquery.Client(project=project)
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        client.query(probe, job_config=job_config)
        return True
    except Exception as exc:
        logger.warning("SQL dry-run failed: %s", exc)
        return False


def build_results(
    experiments: list,
    query: str,
    project: str,
    platform: str = "firefox_desktop",
) -> dict:
    """Execute query and map results back to per-experiment result dicts.

    Uses positional index aliases (exp_0, exp_1, ...) to avoid slug collisions.
    Inlines the BQ execution so callers don't deal with the intermediate rows.
    """
    client = bigquery.Client(project=project)
    rows = [dict(row) for row in client.query(query).result()]

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
            "platform": platform,
            "eligible_count": int(eligible_count),
            "warnings": exp["targetingSql"].get("warnings", []),
        }
    return results


def write_to_gcs(results: dict, bucket_name: str, folder: str, run_date: str) -> None:
    """Write versioned sizing results to GCS for Experimenter to read.

    Writes only experiments evaluated in this run. Experimenter stores the
    computed_at date and decides whether to update its stored estimate.

    Writes two files (matching the enrollment_funnel pattern):
      - experiment_population_estimates_v1_{date}.json  -- dated archive
      - experiment_population_estimates_v1_latest.json  -- consumed by Experimenter
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    json_str = json.dumps({"v1": results}, indent=2)

    dated_path = f"{folder}/experiment_population_estimates_v1_{run_date}.json"
    bucket.blob(dated_path).upload_from_string(
        json_str, content_type="application/json"
    )
    logger.info("Wrote sizing results to gs://%s/%s", bucket_name, dated_path)

    latest_path = f"{folder}/experiment_population_estimates_v1_latest.json"
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
    computed_at = datetime.now(timezone.utc)
    rows = [
        {
            "slug": slug,
            "platform": data["platform"],
            "eligible_count": data["eligible_count"],
            "warnings": data["warnings"],
            "computed_at": computed_at.isoformat(),
        }
        for slug, data in results.items()
    ]
    if not rows:
        return

    # Partition decorator must match computed_at's date so BQ accepts the rows.
    partition_decorator = computed_at.strftime("%Y%m%d")
    table_ref = f"{project}.{dataset}.{table}${partition_decorator}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("slug", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("eligible_count", "INTEGER"),
            bigquery.SchemaField("warnings", "STRING", mode="REPEATED"),
            bigquery.SchemaField("computed_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    logger.info("Wrote %d rows to %s", len(rows), table_ref)


def main():
    """Run Nimbus pre-launch population sizing ETL."""
    args = parser.parse_args()
    run_date = date.fromisoformat(args.date)

    all_experiments = fetch_experiments(args.api_url)
    logger.info("Found %d experiments needing sizing update", len(all_experiments))

    if not all_experiments:
        logger.info("No experiments to process")
        return

    # Group experiments by platform
    platform_groups: dict[str, list] = {}
    for exp in all_experiments:
        app_name = exp.get("appName", "firefox_desktop")
        if app_name not in APP_TARGETING_TABLES:
            logger.warning(
                "Unknown appName %r for %s, skipping", app_name, exp["slug"]
            )
            continue
        platform_groups.setdefault(app_name, []).append(exp)

    all_results: dict = {}
    has_valid = False

    for app_name, experiments in platform_groups.items():
        table = APP_TARGETING_TABLES[app_name]
        is_mobile = app_name in _MOBILE_APPS

        valid_experiments = []
        for exp in experiments:
            sql = exp["targetingSql"]["sql"]
            if _dry_run_sql(sql, project=args.project, table=table):
                valid_experiments.append(exp)
            else:
                logger.warning(
                    "Skipping %s -- SQL failed dry-run validation", exp["slug"]
                )

        if not valid_experiments:
            continue

        has_valid = True
        query = build_query(
            valid_experiments,
            submission_date=run_date,
            table=table,
            is_mobile=is_mobile,
        )

        if args.dry_run:
            print(f"-- {app_name}\n{query}")
            continue

        results = build_results(
            valid_experiments, query, project=args.project, platform=app_name
        )
        logger.info(
            "Got sizing results for %d %s experiments", len(results), app_name
        )
        all_results.update(results)

    if args.dry_run:
        return

    if not has_valid:
        logger.info("No valid experiments after dry-run validation")
        return

    write_to_gcs(
        all_results,
        bucket_name=args.gcs_bucket,
        folder=args.gcs_folder,
        run_date=args.date,
    )
    write_to_bigquery(
        all_results,
        project=args.project,
        dataset=args.output_dataset,
        table=args.output_table,
        submission_date=run_date,
    )
    logger.info("Population sizing ETL complete")


if __name__ == "__main__":
    main()
