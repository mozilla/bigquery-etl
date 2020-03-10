CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_downgrade_parquet`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.telemetry_downgrade_parquet_v1`
