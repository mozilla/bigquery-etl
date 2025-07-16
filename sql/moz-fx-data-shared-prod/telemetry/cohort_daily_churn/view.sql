CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.cohort_daily_churn`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_daily_churn_v1`
