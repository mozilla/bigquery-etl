CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiments_daily_active_clients_v1` (
    submission_date DATE,
    experiment_id STRING,
    branch STRING,
    active_clients INT64
  )
PARTITION BY
  submission_date
CLUSTER BY
  experiment_id,
  branch
