CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_error_aggregates_v1`
AS
SELECT
  submission_date AS submission_date_s3,
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_error_aggregates_v1`
