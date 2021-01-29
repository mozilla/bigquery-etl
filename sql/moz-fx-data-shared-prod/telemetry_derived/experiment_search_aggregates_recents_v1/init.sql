CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_recents_v1`(
    dataset_id STRING,
    branch STRING,
    experiment STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    ad_clicks_count INT64,
    search_with_ads_count INT64,
    search_count INT64
  )
CLUSTER BY
  experiment
