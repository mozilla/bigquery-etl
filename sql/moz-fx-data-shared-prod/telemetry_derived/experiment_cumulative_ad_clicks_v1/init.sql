CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_ad_clicks_v1`(
    time TIMESTAMP,
    experiment STRING,
    branch STRING,
    value INT64
  )
CLUSTER BY
  experiment
