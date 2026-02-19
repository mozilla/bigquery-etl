CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.addon_aggregates_v2`
AS
SELECT
  submission_date AS submission_date_s3,
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.addon_aggregates_v2`
