-- Query for glean_telemetry_derived.cohort_weekly_active_clients_staging_v1
SELECT DISTINCT
  client_id,
  submission_date
FROM
  `moz-fx-data-shared-prod.glean_telemetry.active_users`
WHERE
  submission_date = @submission_date
  AND is_dau IS TRUE
