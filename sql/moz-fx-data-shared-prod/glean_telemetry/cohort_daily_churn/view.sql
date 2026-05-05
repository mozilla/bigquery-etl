CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_telemetry.cohort_daily_churn`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_telemetry_derived.cohort_daily_churn_v1`
