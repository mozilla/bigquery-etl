CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_by_browser_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_by_browser_aggregates_v1`
