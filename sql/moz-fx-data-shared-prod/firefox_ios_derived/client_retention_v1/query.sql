WITH clients_daily AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel AS channel,
    country,
    first_seen_date,
    is_new_profile,
  FROM firefox_ios.baseline_clients_last_seen
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
),

active_users AS (
  SELECT
    submission_date,
    client_id,
    is_dau,
    is_daily_user,
    mozfun.bits28.retention(days_seen_bits, submission_date) AS retention_seen,
    mozfun.bits28.retention(days_seen_bits, submission_date) AS retention_active,
  FROM backfills_staging_derived.active_users
  WHERE
    submission_date = @submission_date
    AND app_name LIKE "Firefox iOS%"
)

SELECT
  @submission_date AS submission_date,
  retention_seen.day_27.metric_date AS metric_date,
  -- clients_daily.submission_date AS metric_date,
  client_id,
  clients_daily.sample_id,
  clients_daily.channel,
  clients_daily.country,
  clients_daily.first_seen_date,
  -- clients_daily.is_new_profile AS new_client,
  -- ping sent retention
  active_users.is_daily_user AS day_ping,
  active_users.is_daily_user AND retention_seen.day_27.active_in_week_3 as sent_ping_week_4,
  -- activity retention
  active_users.is_dau AS day_active,
  active_users.is_dau AND active_users.retention_active.day_27.active_in_week_3 AS retained_week_4,
  -- new client retention
  retention_seen.day_27.metric_date = first_seen_date AS new_client,
  retention_seen.day_27.metric_date = first_seen_date AND retention_active.day_27.active_in_week_3 as new_profile_retained_week_4
  -- clients_daily.is_new_profile AS new_client,
  -- clients_daily.is_new_profile AND retention_active.day_27.active_in_week_3 as new_profile_retained_week_4
  -- active_users.retention_seen,
  -- active_users.retention_active,
FROM clients_daily
INNER JOIN active_users USING(client_id)
