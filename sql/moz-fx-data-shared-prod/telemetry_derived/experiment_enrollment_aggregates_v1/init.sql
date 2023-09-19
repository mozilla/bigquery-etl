CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.analysis.experiment_enrollment_aggregates` (
    type STRING,
    experiment STRING,
    branch STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    enroll_count INT64,
    unenroll_count INT64,
    graduate_count INT64,
    update_count INT64,
    enroll_failed_count INT64,
    unenroll_failed_count INT64,
    update_failed_count INT64,
    disqualification_count INT64,
    exposure_count INT64,
    validation_failed_count INT64
  )
PARTITION BY
  DATE(window_start) CLUSTER BY experiment,
  branch
