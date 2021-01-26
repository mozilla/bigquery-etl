SELECT
  window_start AS `time`,
  experiment,
  branch,
  ad_clicks_count AS value
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
