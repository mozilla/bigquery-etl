CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.adjust.adjust_cohort`
AS
SELECT
  cohort_start_month,
  period_length,
  period,
  app,
  network,
  network_token,
  country,
  os,
  cohort_size,
  retained_users,
  retention_rate,
  time_spent_per_user,
  time_spent_per_session,
  time_spent,
  sessions_per_user,
  sessions,
  date
FROM
  `moz-fx-data-shared-prod.adjust_derived.adjust_cohort_v1`
WHERE
  (network != "Untrusted Devices")
  AND date IN (SELECT MAX(date) FROM `moz-fx-data-shared-prod.adjust_derived.adjust_cohort_v1`)
