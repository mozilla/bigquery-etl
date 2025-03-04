CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.cohort_weekly_statistics`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_weekly_statistics_v1`
