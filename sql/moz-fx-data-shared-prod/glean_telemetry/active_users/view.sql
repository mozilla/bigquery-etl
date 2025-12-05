CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_telemetry.active_users`
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
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
WHERE
  client_id <> '00000000-0000-0000-0000-000000000000' --exclude nil client ID
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
  FALSE AS is_desktop,
  is_mobile
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_active_users`
WHERE
  client_id <> '00000000-0000-0000-0000-000000000000' --exclude nil client ID
