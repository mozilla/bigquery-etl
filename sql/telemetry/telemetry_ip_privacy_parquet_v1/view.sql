CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_ip_privacy_parquet_v1` AS
SELECT
  submission_date_s3 AS submission_date,
  *
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.telemetry_ip_privacy_parquet_v1`
