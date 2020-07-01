CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_mobile_event_parquet`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.telemetry_mobile_event_parquet_v2`
