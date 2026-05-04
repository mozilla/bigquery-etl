CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_telemetry.new_profile_churn_clients`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_telemetry_derived.recent_new_profile_churn_clients_v1`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_telemetry_derived.new_profile_churn_clients_v1`
WHERE
  cohort_date < (
    SELECT
      MIN(cohort_date)
    FROM
      `moz-fx-data-shared-prod.glean_telemetry_derived.recent_new_profile_churn_clients_v1`
  )
