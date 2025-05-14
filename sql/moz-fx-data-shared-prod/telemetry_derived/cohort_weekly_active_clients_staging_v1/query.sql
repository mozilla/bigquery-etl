SELECT DISTINCT
  client_id,
  DATE_TRUNC(submission_date, WEEK) AS activity_date_week
FROM
  `moz-fx-data-shared-prod.telemetry.active_users`
WHERE
  submission_date >= DATE_TRUNC(
    DATE_SUB(@submission_date, INTERVAL 768 day),
    WEEK
  ) --start of week for date 768 days ago
  AND submission_date <= DATE_SUB(
    DATE_TRUNC(@submission_date, WEEK),
    INTERVAL 1 DAY
  ) --through end of last completed week
  AND is_dau IS TRUE;
