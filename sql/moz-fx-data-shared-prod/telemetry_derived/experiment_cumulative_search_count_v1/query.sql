SELECT
  window_start AS `time`,
  experiment,
  branch,
  cumulative_search_count AS value
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
