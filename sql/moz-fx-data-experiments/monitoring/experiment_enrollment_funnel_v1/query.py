#!/usr/bin/env python3

"""Export enrollment funnel rollup to GCS for Experimenter visualization and alerting.

Queries the nimbus_targeting_context ping tables for firefox_desktop, firefox_ios,
and fenix to compute an enrollment funnel snapshot for each live recipe.

For each client-recipe pair, takes the client's most recent evaluation (latest
submission_timestamp) and groups by (app_name, slug, status, reason, conflict_slug)
to produce a count of distinct clients at each funnel stage.

Exports as JSON to GCS for the Experimenter ingestion task to consume.
"""

import json
from argparse import ArgumentParser
from datetime import date

from google.cloud import bigquery, storage

MIN_START_QUERY = """
SELECT MIN(start_date) AS min_start_date
FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
WHERE start_date IS NOT NULL
  AND (end_date IS NULL OR end_date >= DATE_SUB(@run_date, INTERVAL 90 DAY))
"""

FUNNEL_QUERY = """
WITH active_experiments AS (
  -- Experiments that are live or ended within the last 90 days.
  SELECT normandy_slug AS slug
  FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE start_date IS NOT NULL
    AND (end_date IS NULL OR end_date >= DATE_SUB(@run_date, INTERVAL 90 DAY))
),
all_apps_raw AS (
  -- firefox_desktop
  SELECT
    'firefox_desktop' AS app_name,
    t.client_info.client_id AS client_id,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug') AS slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'status') AS status,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'reason') AS reason,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'conflict_slug') AS conflict_slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'branch') AS branch,
    t.submission_timestamp
  FROM `moz-fx-data-shared-prod.firefox_desktop.nimbus_targeting_context` t,
  UNNEST(t.events) AS event
  WHERE DATE(t.submission_timestamp) BETWEEN @min_start_date AND @run_date
    AND t.sample_id = 0
    AND event.category = 'nimbus_events'
    AND event.name = 'enrollment_status'

  UNION ALL

  -- firefox_ios
  SELECT
    'firefox_ios' AS app_name,
    t.client_info.client_id AS client_id,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug') AS slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'status') AS status,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'reason') AS reason,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'conflict_slug') AS conflict_slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'branch') AS branch,
    t.submission_timestamp
  FROM `moz-fx-data-shared-prod.org_mozilla_ios_firefox.nimbus_targeting_context` t,
  UNNEST(t.events) AS event
  WHERE DATE(t.submission_timestamp) BETWEEN @min_start_date AND @run_date
    AND t.sample_id = 0
    AND event.category = 'nimbus_events'
    AND event.name = 'enrollment_status'

  UNION ALL

  -- fenix
  SELECT
    'fenix' AS app_name,
    t.client_info.client_id AS client_id,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug') AS slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'status') AS status,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'reason') AS reason,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'conflict_slug') AS conflict_slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'branch') AS branch,
    t.submission_timestamp
  FROM `moz-fx-data-shared-prod.fenix.nimbus_targeting_context` t,
  UNNEST(t.events) AS event
  WHERE DATE(t.submission_timestamp) BETWEEN @min_start_date AND @run_date
    AND t.sample_id = 0
    AND event.category = 'nimbus_events'
    AND event.name = 'enrollment_status'
),
latest AS (
  -- Keep only the most recent evaluation per client per recipe.
  SELECT
    * EXCEPT (submission_timestamp),
    ROW_NUMBER() OVER (
      PARTITION BY app_name, client_id, slug
      ORDER BY submission_timestamp DESC
    ) AS rn
  FROM all_apps_raw
)
SELECT
  app_name,
  slug,
  branch,
  status,
  reason,
  conflict_slug,
  COUNT(DISTINCT client_id) * 100 AS clients
FROM latest
INNER JOIN active_experiments USING (slug)
WHERE rn = 1
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY slug, app_name, branch, status, reason
"""

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
parser.add_argument("--project", default="moz-fx-data-experiments")
parser.add_argument(
    "--gcs_folder",
    default="enrollment_counts",
    help="GCS folder name for storing exported data (default: enrollment_counts)",
)


def main():
    """Export enrollment funnel data to GCS for Experimenter."""
    args = parser.parse_args()
    run_date = date.fromisoformat(args.date)

    bq_client = bigquery.Client(args.project)

    # Resolve min_start_date in Python first so BQ can prune partitions
    # in the main query using literal date bounds rather than a dynamic subquery.
    min_start_rows = list(
        bq_client.query(
            MIN_START_QUERY,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                ]
            ),
        ).result()
    )
    min_start_date = min_start_rows[0]["min_start_date"] if min_start_rows else None
    if min_start_date is None:
        # No active experiments — write empty files and exit cleanly.
        storage_client = storage.Client(args.project)
        bucket = storage_client.bucket("mozanalysis")
        empty = json.dumps({"v1": {}})
        for path in [
            f"{args.gcs_folder}/enrollment_funnel_v1_{args.date}.json",
            f"{args.gcs_folder}/enrollment_funnel_v1_latest.json",
        ]:
            bucket.blob(path).upload_from_string(empty, content_type="application/json")
        return

    rows = list(
        bq_client.query(
            FUNNEL_QUERY,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_date", "DATE", run_date),
                    bigquery.ScalarQueryParameter(
                        "min_start_date", "DATE", min_start_date
                    ),
                ]
            ),
        ).result()
    )

    # Aggregate into per-experiment structure:
    # { slug: [ {app_name, status, reason, conflict_slug, clients}, ... ] }
    data = {}
    for row in rows:
        slug = row["slug"]
        if slug not in data:
            data[slug] = []
        data[slug].append(
            {
                "app_name": row["app_name"],
                "branch": row["branch"],
                "status": row["status"],
                "reason": row["reason"],
                "conflict_slug": row["conflict_slug"],
                "clients": int(row["clients"]),
            }
        )

    versioned_data = {"v1": data}

    # Upload to GCS — same bucket and folder as enrollment alert data.
    storage_client = storage.Client(args.project)
    bucket = storage_client.bucket("mozanalysis")
    json_str = json.dumps(versioned_data)

    # Dated version for historical reference.
    dated_path = f"{args.gcs_folder}/enrollment_funnel_v1_{args.date}.json"
    bucket.blob(dated_path).upload_from_string(
        json_str, content_type="application/json"
    )

    # Latest version consumed by Experimenter.
    latest_path = f"{args.gcs_folder}/enrollment_funnel_v1_latest.json"
    source_blob = bucket.blob(dated_path)
    bucket.copy_blob(source_blob, bucket, latest_path)


if __name__ == "__main__":
    main()
