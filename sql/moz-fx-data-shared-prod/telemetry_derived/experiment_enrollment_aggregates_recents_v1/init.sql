CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_recents_v1`(
    timestamp TIMESTAMP,
    `type` STRING,
    branch STRING,
    experiment STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    enroll_count INT64,
    unenroll_count INT64,
    graduate_count INT64,
    update_count INT64,
    enroll_failed_count INT64,
    unenroll_failed_count INT64,
    update_failed_count INT64
  )
CLUSTER BY
  experiment
