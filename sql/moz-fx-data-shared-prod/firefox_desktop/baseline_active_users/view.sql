CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
AS
SELECT
  * EXCEPT (
    app_display_version,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    windows_build_number
  ) REPLACE(
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale
  ),
  CASE
    WHEN isp = 'BrowserStack'
      THEN CONCAT('Firefox Desktop', ' ', isp)
    WHEN distribution_id = 'MozillaOnline'
      THEN CONCAT('Firefox Desktop', ' ', distribution_id)
    ELSE 'Firefox Desktop'
  END AS app_name,
  app_display_version AS app_version,
  normalized_channel AS channel,
  normalized_os AS os,
  COALESCE(
    `mozfun.norm.windows_version_info`(normalized_os, normalized_os_version, windows_build_number),
    normalized_os_version
  ) AS os_version_build,
  COALESCE(
    CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
    0
  ) AS os_version_major,
  COALESCE(
    CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
    0
  ) AS os_version_minor,
  CASE
    WHEN BIT_COUNT(days_desktop_active_bits)
      BETWEEN 1
      AND 6
      THEN 'infrequent_user'
    WHEN BIT_COUNT(days_desktop_active_bits)
      BETWEEN 7
      AND 13
      THEN 'casual_user'
    WHEN BIT_COUNT(days_desktop_active_bits)
      BETWEEN 14
      AND 20
      THEN 'regular_user'
    WHEN BIT_COUNT(days_desktop_active_bits) >= 21
      THEN 'core_user'
    ELSE 'other'
  END AS segment_dau,
  CASE
    WHEN BIT_COUNT(days_seen_bits)
      BETWEEN 1
      AND 6
      THEN 'infrequent_user'
    WHEN BIT_COUNT(days_seen_bits)
      BETWEEN 7
      AND 13
      THEN 'casual_user'
    WHEN BIT_COUNT(days_seen_bits)
      BETWEEN 14
      AND 20
      THEN 'regular_user'
    WHEN BIT_COUNT(days_seen_bits) >= 21
      THEN 'core_user'
    ELSE 'other'
  END AS activity_segment,
  EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0, FALSE) AS is_dau,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 7, FALSE) AS is_wau,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 28, FALSE) AS is_mau
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen`
