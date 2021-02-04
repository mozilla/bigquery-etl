CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`(
    dataset_id STRING,
    experiment STRING,
    branch STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    search_count INT64,
    search_with_ads_count INT64,
    ad_clicks_count INT64
  )
PARTITION BY
  DATE(window_start)
CLUSTER BY
  experiment,
  branch
