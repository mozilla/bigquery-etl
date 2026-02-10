CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_ip_privacy_parquet`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.telemetry_ip_privacy_parquet_v1`
