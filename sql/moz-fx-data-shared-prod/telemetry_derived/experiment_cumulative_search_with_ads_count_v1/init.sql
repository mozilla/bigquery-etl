CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_search_with_ads_count_v1`(
    time TIMESTAMP,
    experiment STRING,
    branch STRING,
    value INT64
  )
CLUSTER BY
  experiment
