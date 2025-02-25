CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
AS
SELECT
  * EXCEPT (app_display_version, normalized_channel, normalized_os, normalized_os_version) REPLACE(
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale
  ),
  CASE
    WHEN LOWER(IFNULL(isp, '')) = 'browserstack'
      THEN CONCAT('Firefox Desktop', ' ', isp)
    WHEN LOWER(IFNULL(distribution_id, '')) = 'mozillaonline'
      THEN CONCAT('Firefox Desktop', ' ', distribution_id)
    ELSE 'Firefox Desktop'
  END AS app_name,
  app_display_version AS app_version,
  normalized_channel AS channel,
  normalized_os AS os,
  normalized_os_version AS os_version,
  `mozfun.norm.truncate_version`(normalized_os_version, "major") AS os_version_major,
  `mozfun.norm.truncate_version`(normalized_os_version, "minor") AS os_version_minor,
  COALESCE(
    `mozfun.norm.windows_version_info`(normalized_os, normalized_os_version, windows_build_number),
    normalized_os_version
  ) AS os_version_build,
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
  END AS activity_segment,
  EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  COALESCE(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0, FALSE) AS is_dau,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 7, FALSE) AS is_wau,
  COALESCE(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 28, FALSE) AS is_mau,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen`
