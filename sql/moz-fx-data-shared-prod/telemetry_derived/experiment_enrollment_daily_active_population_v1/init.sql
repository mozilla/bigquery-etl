CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_daily_active_population_v1`(
    time TIMESTAMP,
    experiment STRING,
    branch STRING,
    value INT64
  )
CLUSTER BY
  experiment
