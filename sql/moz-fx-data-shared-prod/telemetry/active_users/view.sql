CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users`
AS
-- Distribution_id is only collected for Desktop and Fenix as of Q1/2024.
-- See e.g. Firefox iOs https://dictionary.telemetry.mozilla.org/apps/firefox_ios?page=1&search=distribution_id
WITH fenix_distribution_id AS (
  SELECT
    client_id,
    sample_id,
    distribution_id,
  FROM
    `moz-fx-data-shared-prod.fenix.firefox_android_clients`
)
-- Firefox Desktop (Legacy)
SELECT
  submission_date,
  client_id,
  sample_id,
  CASE
    WHEN isp_name = 'BrowserStack'
      THEN CONCAT(app_name, ' ', isp_name)
    WHEN distribution_id = 'MozillaOnline'
      THEN CONCAT(app_name, ' ', distribution_id)
    ELSE app_name
  END AS app_name,
  days_seen_bits,
  days_active_bits,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  IF(
    LOWER(IFNULL(isp_name, '')) <> "browserstack"
    AND LOWER(distribution_id) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop,
  FALSE AS is_mobile
FROM
  `moz-fx-data-shared-prod.backfills_staging_derived.telemetry_derived_clients_last_seen_v2_20240322_view`
UNION ALL
-- Fenix
SELECT
  submission_date,
  client_id,
  sample_id,
  CASE
    WHEN isp = 'BrowserStack'
      THEN CONCAT('Fenix ', isp)
    WHEN distribution_id = 'MozillaOnline'
      THEN CONCAT('Fenix', ' ', distribution_id)
    ELSE 'Fenix'
  END AS app_name,
  days_seen_bits,
  days_active_bits,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  FALSE AS is_desktop,
  IF(
    LOWER(IFNULL(isp, '')) <> "browserstack"
    AND LOWER(distribution_id) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_mobile
FROM
  `moz-fx-data-shared-prod.backfills_staging_derived.fenix_baseline_clients_last_seen_20240325`
LEFT JOIN
  fenix_distribution_id
  USING (client_id, sample_id)
UNION ALL
-- Firefox iOS
SELECT
  submission_date,
  client_id,
  sample_id,
  CASE
    WHEN isp = 'BrowserStack'
      THEN CONCAT('Firefox iOS', ' ', isp)
    ELSE 'Firefox iOS'
  END AS app_name,
  days_seen_bits,
  days_active_bits,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  FALSE AS is_desktop,
  IF(LOWER(IFNULL(isp, '')) <> "browserstack", TRUE, FALSE) AS is_mobile
FROM
  `moz-fx-data-shared-prod.backfills_staging_derived.firefox_ios_baseline_clients_last_seen_20240325`
UNION ALL
-- Klar Android
SELECT
  submission_date,
  client_id,
  sample_id,
  CASE
    WHEN isp = 'BrowserStack'
      THEN CONCAT('Klar Android', ' ', isp)
    ELSE 'Klar Android'
  END AS app_name,
  days_seen_bits,
  days_active_bits,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  FALSE AS is_desktop,
  FALSE AS is_mobile
FROM
  `moz-fx-data-shared-prod.backfills_staging_derived.klar_android_baseline_clients_last_seen_20240325`
UNION ALL
-- Klar iOS
SELECT
  submission_date,
  client_id,
  sample_id,
  CASE
    WHEN isp = 'BrowserStack'
      THEN CONCAT('Klar iOS', ' ', isp)
    ELSE 'Klar iOS'
  END AS app_name,
  days_seen_bits,
  days_active_bits,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  FALSE AS is_desktop,
  FALSE AS is_mobile
FROM
  `moz-fx-data-shared-prod.backfills_staging_derived.klar_ios_baseline_clients_last_seen_20240325`
UNION ALL
-- Focus Android
SELECT
  submission_date,
  client_id,
  sample_id,
  CASE
    WHEN isp = 'BrowserStack'
      THEN CONCAT('Focus Android', ' ', isp)
    ELSE 'Focus Android'
  END AS app_name,
  days_seen_bits,
  days_active_bits,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) <28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  FALSE AS is_desktop,
  IF(LOWER(IFNULL(isp, '')) <> "browserstack", TRUE, FALSE) AS is_mobile
FROM
  `moz-fx-data-shared-prod.backfills_staging_derived.focus_android_baseline_clients_last_seen_20240325`
UNION ALL
-- Focus iOS
SELECT
  submission_date,
  client_id,
  sample_id,
  CASE
    WHEN isp = 'BrowserStack'
      THEN CONCAT('Focus iOS', ' ', isp)
    ELSE 'Focus iOS'
  END AS app_name,
  days_seen_bits,
  days_active_bits,
  CAST(mozfun.bits28.days_since_seen(days_active_bits) = 0 AS BOOLEAN) AS is_dau,
  CAST(mozfun.bits28.days_since_seen(days_active_bits) < 7 AS BOOLEAN) AS is_wau,
  CAST(mozfun.bits28.days_since_seen(days_active_bits) < 28 AS BOOLEAN) AS is_mau,
  CAST(mozfun.bits28.days_since_seen(days_seen_bits) = 0 AS BOOLEAN) AS is_daily_user,
  CAST(mozfun.bits28.days_since_seen(days_seen_bits) < 7 AS BOOLEAN) AS is_weekly_user,
  CAST(mozfun.bits28.days_since_seen(days_seen_bits) < 28 AS BOOLEAN) AS is_monthly_user,
  FALSE AS is_desktop,
  IF(LOWER(isp) <> "browserstack", TRUE, FALSE) AS is_mobile
FROM
  `moz-fx-data-shared-prod.backfills_staging_derived.focus_ios_baseline_clients_last_seen_20240325`
