CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  app_name,
  days_seen_bits,
  days_active_bits,
  is_dau,
  is_wau,
  is_mau,
  is_daily_user,
  is_weekly_user,
  is_monthly_user,
  is_desktop,
  FALSE AS is_mobile
FROM
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_v2`
WHERE
  submission_date < current_date
UNION ALL
SELECT
  submission_date,
  client_id,
  sample_id,
  app_name,
  days_seen_bits,
  days_active_bits,
  is_dau,
  is_wau,
  is_mau,
  is_daily_user,
  is_weekly_user,
  is_monthly_user,
  is_desktop,
  is_mobile
FROM
  `moz-fx-data-shared-prod.telemetry.active_users_mobile`
WHERE
  submission_date < current_date
