CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_anonymous_parquet`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.telemetry_anonymous_parquet_v1`
