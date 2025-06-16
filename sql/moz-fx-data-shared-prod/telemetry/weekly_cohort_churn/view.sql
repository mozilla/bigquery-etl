CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.weekly_cohort_churn`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.weekly_cohort_churn_v1`
