SELECT DISTINCT
  client_id,
  DATE_TRUNC(submission_date, WEEK) AS activity_date_week
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_weekly_active_clients_staging_v1`
WHERE
  submission_date = DATE_TRUNC(
    DATE_SUB(@submission_date, INTERVAL 768 day),
    WEEK
  ) --start of week for date 768 days ago
  AND submission_date <= DATE_SUB(
    DATE_TRUNC(@submission_date, WEEK),
    INTERVAL 1 DAY
  ) --through end of last completed week
