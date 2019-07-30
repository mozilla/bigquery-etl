CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.telemetry_downgrade_parquet_v1` AS
SELECT
  submission_date_s3 AS submission_date,
  *
FROM
  `moz-fx-data-derived-datasets.telemetry_raw.telemetry_downgrade_parquet_v1`
