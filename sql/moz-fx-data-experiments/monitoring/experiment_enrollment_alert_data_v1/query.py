#!/usr/bin/env python3

"""Export enrollment/unenrollment counts and reasons to GCS for Experimenter alerting.

Queries cumulative enrollment/unenrollment counts by experiment and branch,
plus unenrollment reason breakdown. Exports as JSON to GCS for Experimenter
alerting system to consume. Raw counts only; Experimenter handles computations.
"""

import json
from argparse import ArgumentParser

from google.cloud import bigquery, storage

# BigQuery queries to extract enrollment/unenrollment data
ENROLLMENT_QUERY = """
WITH active_experiments AS (
  SELECT DISTINCT
    normandy_slug as experiment,
    b.slug AS branch
  FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  CROSS JOIN UNNEST(branches) b
  WHERE start_date IS NOT NULL
),
enrollment_totals AS (
  SELECT
    experiment,
    branch,
    SUM(value) as total_enrollments
  FROM `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_overall_v1`
  WHERE experiment IS NOT NULL AND branch IS NOT NULL
  GROUP BY 1, 2
),
unenrollment_totals AS (
  SELECT
    experiment,
    branch,
    SUM(value) as total_unenrollments
  FROM `moz-fx-data-shared-prod.telemetry_derived.experiment_unenrollment_overall_v1`
  WHERE experiment IS NOT NULL AND branch IS NOT NULL
  GROUP BY 1, 2
),
combined_by_experiment_branch AS (
  SELECT
    COALESCE(e.experiment, u.experiment) as experiment,
    COALESCE(e.branch, u.branch) as branch,
    COALESCE(e.total_enrollments, 0) as enrollments,
    COALESCE(u.total_unenrollments, 0) as unenrollments
  FROM enrollment_totals e
  FULL OUTER JOIN unenrollment_totals u
    ON e.experiment = u.experiment AND e.branch = u.branch
)
SELECT
  experiment,
  branch,
  enrollments,
  unenrollments,
  SUM(enrollments) OVER (PARTITION BY experiment) as experiment_total_enrollments,
  SUM(unenrollments) OVER (PARTITION BY experiment) as experiment_total_unenrollments
FROM combined_by_experiment_branch
INNER JOIN active_experiments USING (experiment, branch)
ORDER BY 1, 2
"""

UNENROLLMENT_REASONS_QUERY = """
-- Query unenrollment reasons from the 1% sample (sample_id = 0)
-- This is sufficient for determining relative ranking of reasons for alerts.
-- Alerts will link to Looker dashboard for detailed analysis of actual counts.
WITH active_experiments AS (
  SELECT DISTINCT
    normandy_slug as experiment,
    b.slug AS branch
  FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
  CROSS JOIN UNNEST(branches) b
  WHERE start_date IS NOT NULL
)
SELECT
  active_experiments.experiment,
  active_experiments.branch,
  mozfun.map.get_key(events.event_map_values, 'reason') as reason,
  COUNT(*) as count
FROM active_experiments
LEFT JOIN `mozdata.telemetry.events` events
  ON active_experiments.experiment = events.event_string_value
  AND active_experiments.branch = mozfun.map.get_key(events.event_map_values, 'branch')
  AND events.event_category = 'normandy'
  AND events.event_method LIKE 'unenroll%'
  AND events.sample_id = 0
GROUP BY 1, 2, 3
HAVING reason IS NOT NULL
ORDER BY 1, 4 DESC
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
    """Export enrollment data to GCS for Experimenter alerting."""
    args = parser.parse_args()

    bq_client = bigquery.Client(args.project)

    enrollment_rows = [dict(row) for row in bq_client.query(ENROLLMENT_QUERY).result()]
    reason_rows = [
        dict(row) for row in bq_client.query(UNENROLLMENT_REASONS_QUERY).result()
    ]

    # Aggregate into per-experiment structure
    data = {}
    for row in enrollment_rows:
        exp = row["experiment"]
        if exp not in data:
            data[exp] = {
                "total_enrollments": int(row["experiment_total_enrollments"]),
                "total_unenrollments": int(row["experiment_total_unenrollments"]),
                "branches": {},
                "unenrollment_reasons": {},
            }
        branch = row["branch"]
        data[exp]["branches"][branch] = {
            "enrollments": int(row["enrollments"]),
            "unenrollments": int(row["unenrollments"]),
        }

    # Add unenrollment reasons by branch
    # Note: counts are from 1% sample (sample_id = 0) for performance
    # These are used for relative ranking only; alerts link to Looker for detailed analysis
    for row in reason_rows:
        exp = row["experiment"]
        if exp in data:
            branch = row["branch"]
            reason = row["reason"] or "unknown"
            if "reasons_by_branch" not in data[exp]:
                data[exp]["reasons_by_branch"] = {}
            if branch not in data[exp]["reasons_by_branch"]:
                data[exp]["reasons_by_branch"][branch] = {}
            data[exp]["reasons_by_branch"][branch][reason] = {
                "1pct_count": int(row["count"])
            }

    # Wrap in versioning schema
    versioned_data = {"v1": data}

    # Upload to GCS
    storage_client = storage.Client(args.project)
    bucket = storage_client.bucket("mozanalysis")
    json_str = json.dumps(versioned_data)

    # Dated version
    dated_path = f"{args.gcs_folder}/enrollment_counts_{args.date}.json"
    bucket.blob(dated_path).upload_from_string(
        json_str, content_type="application/json"
    )

    # Latest version (copy dated version)
    latest_path = f"{args.gcs_folder}/enrollment_counts_latest.json"
    source_blob = bucket.blob(dated_path)
    bucket.copy_blob(source_blob, bucket, latest_path)


if __name__ == "__main__":
    main()
