CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.analysis.experiment_search_aggregates_v1`(
    experiment STRING,
    branch STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    search_counts INT64,
    search_with_ads_count INT64,
    ad_clicks_count INT64
  )
PARTITION BY
  DATE(window_start)
CLUSTER BY
  experiment,
  branch
