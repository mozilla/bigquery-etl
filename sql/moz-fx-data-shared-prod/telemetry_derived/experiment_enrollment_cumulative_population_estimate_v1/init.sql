CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_cumulative_population_estimate_v1`(
    time TIMESTAMP,
    experiment STRING,
    value INT64
  )
CLUSTER BY
  experiment
