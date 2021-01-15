WITH pivot AS (
  SELECT
    window_start,
    experiment,
    branch,
    "graduated" AS event,
    graduate_count AS count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live`
  UNION ALL
  SELECT
    window_start,
    experiment,
    branch,
    "enroll_failed" AS event,
    enroll_failed_count AS count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live`
  UNION ALL
  SELECT
    window_start,
    experiment,
    branch,
    "unenroll_failed" AS event,
    unenroll_failed_count AS count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live`
  UNION ALL
  SELECT
    window_start,
    experiment,
    branch,
    "updated" AS event,
    update_count AS count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live`
  UNION ALL
  SELECT
    window_start,
    experiment,
    branch,
    "update_failed" AS event,
    update_failed_count AS count
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live`
)
SELECT
  window_start AS `time`,
  experiment,
  branch,
  event,
  SUM(`count`) AS value
FROM
  pivot
GROUP BY
  1,
  2,
  3,
  4
