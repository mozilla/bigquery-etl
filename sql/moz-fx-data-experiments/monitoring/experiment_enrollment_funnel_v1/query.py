#!/usr/bin/env python3

"""Export enrollment funnel rollup to GCS for Experimenter visualization and alerting.

Queries the nimbus_targeting_context ping tables for firefox_desktop, firefox_ios,
and fenix to compute a cumulative enrollment funnel for each live recipe.

For each client-recipe pair, takes the client's most recent evaluation (latest
submission_timestamp) and groups by (app_name, slug, status, reason, conflict_slug)
to produce a count of distinct clients at each funnel stage.

Exports as JSON to GCS for the Experimenter ingestion task to consume.
"""

import json
from argparse import ArgumentParser

from google.cloud import bigquery, storage

FUNNEL_QUERY = """
WITH active_experiments AS (
  -- All experiments with a known start date, including recently ended ones.
  -- We include experiments that ended within the last 90 days so their final
  -- funnel snapshot is preserved until they age out.
  SELECT
    experimenter_slug AS slug,
    COALESCE(start_date, DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) AS start_date,
    COALESCE(end_date, CURRENT_DATE()) AS end_date
  FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  WHERE start_date IS NOT NULL
    AND (end_date IS NULL OR end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY))
),
min_start AS (
  SELECT MIN(start_date) AS min_start_date
  FROM active_experiments
),
-- Latest evaluation per client per recipe for firefox_desktop
desktop_latest AS (
  SELECT
    t.client_info.client_id AS client_id,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug') AS slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'status') AS status,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'reason') AS reason,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'conflict_slug') AS conflict_slug,
    ROW_NUMBER() OVER (
      PARTITION BY
        t.client_info.client_id,
        (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug')
      ORDER BY t.submission_timestamp DESC
    ) AS rn
  FROM `mozdata.firefox_desktop.nimbus_targeting_context` t,
  UNNEST(t.events) AS event,
  min_start
  WHERE DATE(t.submission_timestamp) BETWEEN min_start.min_start_date AND CURRENT_DATE()
    AND event.category = 'nimbus_events'
    AND event.name = 'enrollment_status'
),
-- Latest evaluation per client per recipe for firefox_ios
ios_latest AS (
  SELECT
    t.client_info.client_id AS client_id,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug') AS slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'status') AS status,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'reason') AS reason,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'conflict_slug') AS conflict_slug,
    ROW_NUMBER() OVER (
      PARTITION BY
        t.client_info.client_id,
        (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug')
      ORDER BY t.submission_timestamp DESC
    ) AS rn
  FROM `mozdata.org_mozilla_ios_firefox.nimbus_targeting_context` t,
  UNNEST(t.events) AS event,
  min_start
  WHERE DATE(t.submission_timestamp) BETWEEN min_start.min_start_date AND CURRENT_DATE()
    AND event.category = 'nimbus_events'
    AND event.name = 'enrollment_status'
),
-- Latest evaluation per client per recipe for fenix
fenix_latest AS (
  SELECT
    t.client_info.client_id AS client_id,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug') AS slug,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'status') AS status,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'reason') AS reason,
    (SELECT value FROM UNNEST(event.extra) WHERE key = 'conflict_slug') AS conflict_slug,
    ROW_NUMBER() OVER (
      PARTITION BY
        t.client_info.client_id,
        (SELECT value FROM UNNEST(event.extra) WHERE key = 'slug')
      ORDER BY t.submission_timestamp DESC
    ) AS rn
  FROM `mozdata.org_mozilla_fenix.nimbus_targeting_context` t,
  UNNEST(t.events) AS event,
  min_start
  WHERE DATE(t.submission_timestamp) BETWEEN min_start.min_start_date AND CURRENT_DATE()
    AND event.category = 'nimbus_events'
    AND event.name = 'enrollment_status'
),
combined AS (
  SELECT 'firefox_desktop' AS app_name, slug, status, reason, conflict_slug, client_id
  FROM desktop_latest
  WHERE rn = 1

  UNION ALL

  SELECT 'firefox_ios' AS app_name, slug, status, reason, conflict_slug, client_id
  FROM ios_latest
  WHERE rn = 1

  UNION ALL

  SELECT 'fenix' AS app_name, slug, status, reason, conflict_slug, client_id
  FROM fenix_latest
  WHERE rn = 1
)
SELECT
  app_name,
  slug,
  status,
  reason,
  conflict_slug,
  COUNT(DISTINCT client_id) AS clients
FROM combined
INNER JOIN active_experiments USING (slug)
GROUP BY 1, 2, 3, 4, 5
ORDER BY slug, app_name, status, reason
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

    bq_client = bigquery.Client(args.project)
    rows = [dict(row) for row in bq_client.query(FUNNEL_QUERY).result()]

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
                "status": row["status"],
                "reason": row["reason"],
                "conflict_slug": row["conflict_slug"],
                "clients": int(row["clients"]),
            }
        )

    versioned_data = {"v1": data}

    # Upload to GCS — same bucket and folder as enrollment alert data
    storage_client = storage.Client(args.project)
    bucket = storage_client.bucket("mozanalysis")
    json_str = json.dumps(versioned_data)

    # Dated version for historical reference
    dated_path = f"{args.gcs_folder}/enrollment_funnel_{args.date}.json"
    bucket.blob(dated_path).upload_from_string(
        json_str, content_type="application/json"
    )

    # Latest version consumed by Experimenter
    latest_path = f"{args.gcs_folder}/enrollment_funnel_latest.json"
    source_blob = bucket.blob(dated_path)
    bucket.copy_blob(source_blob, bucket, latest_path)


if __name__ == "__main__":
    main()
