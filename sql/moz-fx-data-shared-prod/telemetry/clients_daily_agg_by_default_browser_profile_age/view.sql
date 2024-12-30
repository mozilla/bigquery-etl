CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_daily_agg_by_default_browser_profile_age`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_agg_by_default_browser_profile_age_v1`
