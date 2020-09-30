CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.eng_workflow_hgpush_parquet_v1`
AS
SELECT
  submission_date AS submission_date_s3,
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.eng_workflow_hgpush_parquet_v1`
