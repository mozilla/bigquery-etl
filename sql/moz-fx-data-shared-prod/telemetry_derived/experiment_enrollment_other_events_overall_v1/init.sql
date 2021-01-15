CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_other_events_overall_v1`(
    time TIMESTAMP,
    experiment STRING,
    branch STRING,
    event STRING,
    value INT64
  )
CLUSTER BY
  experiment
