CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.urlbar_search_sessions_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.urlbar_search_sessions_daily_v1`
