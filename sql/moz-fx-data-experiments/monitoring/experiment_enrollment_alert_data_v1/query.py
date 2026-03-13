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
WITH enrollment_totals AS (
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
ORDER BY 1, 2
"""

UNENROLLMENT_REASONS_QUERY = """
WITH reason_breakdown AS (
  SELECT
    experiment,
    other_details as reason,
    COUNT(*) as count
  FROM `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_other_events_overall_v1`
  WHERE experiment IS NOT NULL AND other_details IS NOT NULL
  GROUP BY 1, 2
)
SELECT experiment, reason, count
FROM reason_breakdown
WHERE reason IS NOT NULL
ORDER BY 1, 2, 3 DESC
"""

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
parser.add_argument("--project", default="moz-fx-data-experiments")
parser.add_argument(
    "--dry_run", action="store_true", help="Print output, skip GCS upload"
)


def main():
    """Export enrollment data to GCS for Experimenter alerting."""
    args = parser.parse_args()

    bq_client = bigquery.Client(args.project)

    print("Querying enrollment/unenrollment data...")
    enrollment_rows = [dict(row) for row in bq_client.query(ENROLLMENT_QUERY).result()]
    print(f"Retrieved {len(enrollment_rows)} branch records")

    print("Querying unenrollment reasons...")
    reason_rows = [
        dict(row) for row in bq_client.query(UNENROLLMENT_REASONS_QUERY).result()
    ]
    print(f"Retrieved {len(reason_rows)} reason records")

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

    # Add unenrollment reasons
    for row in reason_rows:
        exp = row["experiment"]
        if exp in data:
            reason = row["reason"] or "unknown"
            data[exp]["unenrollment_reasons"][reason] = int(row["count"])

    print(f"Aggregated data for {len(data)} experiments")

    # Upload to GCS
    if not args.dry_run:
        storage_client = storage.Client(args.project)
        bucket = storage_client.bucket("mozanalysis")
        json_str = json.dumps(data, indent=2)

        # Dated version
        dated_path = f"enrollment_alerts/enrollment_alerts_{args.date}.json"
        bucket.blob(dated_path).upload_from_string(
            json_str, content_type="application/json"
        )
        print(f"Uploaded to gs://mozanalysis/{dated_path}")

        # Latest version
        latest_path = "enrollment_alerts/enrollment_alerts_latest.json"
        bucket.blob(latest_path).upload_from_string(
            json_str, content_type="application/json"
        )
        print(f"Uploaded to gs://mozanalysis/{latest_path}")
    else:
        print("[DRY RUN] Skipped GCS upload")
        print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
