CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.events_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.events_daily_v1`
