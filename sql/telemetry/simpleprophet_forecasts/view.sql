CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.simpleprophet_forecasts`
AS
SELECT * FROM
  `moz-fx-data-shared-prod.telemetry_derived.simpleprophet_forecasts_desktop_v1`
UNION ALL
SELECT * FROM
  `moz-fx-data-shared-prod.telemetry_derived.simpleprophet_forecasts_mobile_v1`
UNION ALL
SELECT * FROM
  `moz-fx-data-shared-prod.telemetry_derived.simpleprophet_forecasts_fxa_v1`
