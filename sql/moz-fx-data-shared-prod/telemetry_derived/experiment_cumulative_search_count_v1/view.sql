CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_search_count_v1`
AS
SELECT
  window_start AS `time`,
  experiment,
  branch,
  cumulative_search_count AS value
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
