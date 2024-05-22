CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.telemetry.active_users`
AS
-- Firefox Desktop
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
-- Fenix
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
  `moz-fx-data-shared-prod.fenix.active_users`
WHERE
  submission_date < current_date
UNION ALL
-- Firefox iOS
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
  `moz-fx-data-shared-prod.firefox_ios.active_users`
WHERE
  submission_date < current_date
UNION ALL
-- Focus Android
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
  `moz-fx-data-shared-prod.focus_android.active_users`
WHERE
  submission_date < current_date
UNION ALL
-- Focus iOS
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
  `moz-fx-data-shared-prod.focus_ios.active_users`
WHERE
  submission_date < current_date
UNION ALL
-- Klar Android
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
  `moz-fx-data-shared-prod.klar_android.active_users`
WHERE
  submission_date < current_date
UNION ALL
-- Klar iOS
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
  `moz-fx-data-shared-prod.klar_ios.active_users`
WHERE
  submission_date < current_date
