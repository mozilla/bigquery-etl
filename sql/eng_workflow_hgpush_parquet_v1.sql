
--
CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.eng_workflow_hgpush_parquet_v1` AS
SELECT
  submission_date_s3 AS submission_date,
  *
FROM
  `moz-fx-data-derived-datasets.telemetry_raw.eng_workflow_hgpush_parquet_v1`
