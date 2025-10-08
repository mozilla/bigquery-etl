CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_telemetry.cohort_weekly_statistics`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_telemetry_derived.cohort_weekly_statistics_v1`
