CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.addons_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.addons_daily_v1`
