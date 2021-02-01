CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_cumulative_ad_clicks`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_cumulative_ad_clicks_v1`
