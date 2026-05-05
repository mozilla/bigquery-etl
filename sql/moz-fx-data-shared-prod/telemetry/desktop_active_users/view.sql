CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_active_users`
AS
SELECT
  submission_date,
  client_id,
  profile_group_id,
  sample_id,
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
  days_seen_bits,
  days_active_bits,
  CASE
    WHEN isp_name = 'BrowserStack'
      THEN CONCAT('Firefox Desktop', ' ', isp_name)
    WHEN distribution_id = 'MozillaOnline'
      THEN CONCAT('Firefox Desktop', ' ', distribution_id)
    ELSE 'Firefox Desktop'
  END AS app_name,
  app_version,
  normalized_channel,
  country,
  city,
  locale,
  first_seen_date,
  os,
  normalized_os_version,
  windows_build_number,
  scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
  scalar_parent_browser_engagement_total_uri_count_sum,
  is_default_browser,
  isp_name,
  distribution_id,
  active_hours_sum,
  attribution.source AS attribution_source,
  attribution.medium AS attribution_medium,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  IF(
    LOWER(IFNULL(isp_name, '')) <> "browserstack"
    AND LOWER(IFNULL(distribution_id, '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v2`
