CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
AS
SELECT
  *,
  CASE
    WHEN LOWER(IFNULL(isp, '')) = 'browserstack'
      THEN CONCAT('Firefox Desktop', ' ', isp)
    WHEN LOWER(IFNULL(distribution_id, '')) = 'mozillaonline'
      THEN CONCAT('Firefox Desktop', ' ', distribution_id)
    ELSE 'Firefox Desktop'
  END AS app_name,
  CASE
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 1
      AND 6
      THEN 'infrequent_user'
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 7
      AND 13
      THEN 'casual_user'
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 14
      AND 20
      THEN 'regular_user'
    WHEN BIT_COUNT(days_active_bits) >= 21
      THEN 'core_user'
    ELSE 'other'
  END AS activity_segment,
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
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau_duration,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau_duration,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau_duration,
  IFNULL(mozfun.bits28.days_since_seen(days_desktop_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_desktop_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  IF(
    LOWER(IFNULL(isp, '')) <> "browserstack"
    AND LOWER(IFNULL(distribution_id, '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_included_kpi
FROM
  `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen`
