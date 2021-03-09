CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_ad_clicks_v1`
AS
SELECT
  window_start AS `time`,
  experiment,
  branch,
  cumulative_ad_clicks_count AS value
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1`
