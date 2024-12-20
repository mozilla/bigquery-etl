{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dau_reporting_active_users_view }}`
AS
SELECT
  submission_date,
  usage_profile_id,
  first_run_date,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  locale,
  app_build,
  app_display_version,
  distribution_id,
  is_active,
  first_seen_date,
  days_seen_bits,
  days_active_bits,
  days_created_profile_bits,
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
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user

--
-- TODO: uncomment once duration is added to the dau_reporting ping
--
-- -- Bit patterns capturing activity dates relative to the submission date.
-- days_seen_session_start_bits,
-- days_seen_session_end_bits,
--

-- -- TODO: verify if these fields are needed
--   app_version,
--   country,
--   city,
--   locale,
--   os,
--   windows_build_number,
--   scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
--   scalar_parent_browser_engagement_total_uri_count_sum,
--   is_default_browser,
--   isp_name,
--   CASE
--     WHEN isp_name = 'BrowserStack'
--       THEN CONCAT('Firefox Desktop', ' ', isp_name)
--     WHEN distribution_id = 'MozillaOnline'
--       THEN CONCAT('Firefox Desktop', ' ', distribution_id)
--     ELSE 'Firefox Desktop'
--   END AS app_name,
--   IF(
--     LOWER(IFNULL(isp_name, '')) <> "browserstack"
--     AND LOWER(IFNULL(distribution_id, '')) <> "mozillaonline",
--     TRUE,
--     FALSE
--   ) AS is_desktop


FROM
  `{{ dau_reporting_clients_daily_table }}`
LEFT JOIN
  `{{ dau_reporting_clients_first_seen_table }}`
  USING (usage_profile_id)
LEFT JOIN
  `{{ dau_reporting_clients_last_seen_table }}`
  USING (usage_profile_id)
