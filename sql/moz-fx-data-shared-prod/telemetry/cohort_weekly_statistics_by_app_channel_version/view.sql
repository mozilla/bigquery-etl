CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.cohort_weekly_statistics_by_app_channel_version`
AS
WITH clients_first_seen AS (
  SELECT
    normalized_app_name,
    normalized_channel,
    app_version,
    DATE_TRUNC(cohort_date, WEEK) AS cohort_date_week,
    client_id
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v2`
  WHERE
    cohort_date >= DATE_TRUNC(
      DATE_SUB(current_date, INTERVAL 180 day),
      WEEK
    ) --start of week for date 180 days ago
    AND normalized_app_name NOT LIKE '%BrowserStack'
    AND normalized_app_name NOT LIKE '%MozillaOnline'
),
submission_date_activity AS (
  SELECT DISTINCT
    client_id,
    submission_date AS activity_date,
    DATE_TRUNC(submission_date, WEEK) AS activity_date_week
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users`
  WHERE
    submission_date > DATE_TRUNC(
      DATE_SUB(current_date, INTERVAL 180 day),
      WEEK
    ) --start of week for date 180 days ago
    AND submission_date <= DATE_SUB(
      DATE_TRUNC(current_date, WEEK),
      INTERVAL 1 DAY
    ) --through last completed week
    AND is_dau IS TRUE
),
clients_first_seen_in_last_180_days_and_activity_next_180_days AS (
  SELECT
    a.normalized_app_name,
    a.normalized_channel,
    a.app_version,
    a.cohort_date_week,
    b.activity_date_week,
    COUNT(DISTINCT(b.client_id)) AS nbr_active_clients
  FROM
    clients_first_seen a
  LEFT JOIN
    submission_date_activity b
    ON a.client_id = b.client_id
  GROUP BY
    a.normalized_app_name,
    a.normalized_channel,
    a.app_version,
    a.cohort_date_week,
    b.activity_date_week
),
--get # of unique clients by cohort start date week, normalized app name, channel, and app version
initial_cohort_counts AS (
  SELECT
    normalized_app_name,
    normalized_channel,
    app_version,
    cohort_date_week,
    COUNT(DISTINCT(client_id)) AS nbr_clients_in_cohort
  FROM
    clients_first_seen
  GROUP BY
    normalized_app_name,
    normalized_channel,
    app_version,
    cohort_date_week
)
SELECT
  i.normalized_app_name,
  i.normalized_channel,
  i.app_version,
  i.cohort_date_week,
  i.nbr_clients_in_cohort,
  a.activity_date_week,
  DATE_DIFF(a.activity_date_week, i.cohort_date_week, WEEK) AS weeks_after_first_seen_week,
  a.nbr_active_clients,
  SAFE_DIVIDE(a.nbr_active_clients, i.nbr_clients_in_cohort) AS pct_retained
FROM
  initial_cohort_counts AS i
LEFT JOIN
  clients_first_seen_in_last_180_days_and_activity_next_180_days AS a
  ON COALESCE(i.normalized_app_name, 'NULL') = COALESCE(a.normalized_app_name, 'NULL')
  AND COALESCE(i.normalized_channel, 'NULL') = COALESCE(a.normalized_channel, 'NULL')
  AND COALESCE(i.app_version, 'NULL') = COALESCE(a.app_version, 'NULL')
  AND i.cohort_date_week = a.cohort_date_week
