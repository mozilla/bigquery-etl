SELECT
  window_start AS `time`,
  experiment,
  branch,
  search_with_ads_count AS value
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
