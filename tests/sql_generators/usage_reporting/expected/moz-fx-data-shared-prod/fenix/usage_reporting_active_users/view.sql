-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.usage_reporting_active_users`
AS
SELECT
  * EXCEPT (submission_date, app_channel, normalized_country_code, app_display_version),
  last_seen.submission_date,
  daily.submission_date AS `date`,
  daily.normalized_channel AS channel,
  IFNULL(daily.normalized_country_code, "??") AS country,
  EXTRACT(YEAR FROM first_seen.first_seen_date) AS first_seen_year,
  CASE
    WHEN LOWER(distribution_id) = "mozillaonline"
      THEN CONCAT("fenix", " ", distribution_id)
    ELSE "fenix"
  END AS app_name,
  -- Activity fields to support metrics built on top of activity
  CASE
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 1
      AND 6
      THEN "infrequent_user"
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 7
      AND 13
      THEN "casual_user"
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 14
      AND 20
      THEN "regular_user"
    WHEN BIT_COUNT(days_active_bits) >= 21
      THEN "core_user"
    ELSE "other"
  END AS activity_segment,
  CAST(`mozfun.norm.extract_version`(os_version, "major") AS INTEGER) AS os_version_major,
  CAST(`mozfun.norm.extract_version`(os_version, "minor") AS INTEGER) AS os_version_minor,
  app_display_version AS app_version,
  `mozfun.norm.browser_version_info`(app_display_version).major_version AS app_version_major,
  `mozfun.norm.browser_version_info`(app_display_version).minor_version AS app_version_minor,
  `mozfun.norm.browser_version_info`(
    app_display_version
  ).patch_revision AS app_version_patch_revision,
  `mozfun.norm.browser_version_info`(
    app_display_version
  ).is_major_release AS app_version_is_major_release,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  -- Metrics based on pings sent
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
FROM
  `moz-fx-data-shared-prod.fenix.usage_reporting_clients_last_seen` AS last_seen
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.usage_reporting_clients_daily` AS daily
  USING (submission_date, usage_profile_id)
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.usage_reporting_clients_first_seen` AS first_seen
  USING (usage_profile_id)
