CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.cohort_daily_statistics`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_daily_statistics_v1`
WHERE
  normalized_app_name not like "%Desktop%"
