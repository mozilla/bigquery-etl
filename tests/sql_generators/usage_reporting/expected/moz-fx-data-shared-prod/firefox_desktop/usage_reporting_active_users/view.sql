-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.usage_reporting_active_users`
AS
WITH first_seen AS (
  SELECT
    usage_profile_id,
    first_seen_date,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.usage_reporting_clients_first_seen`
)
SELECT
  * EXCEPT (submission_date, app_channel, normalized_country_code, app_display_version),
  submission_date,
  submission_date AS `date`,
  normalized_channel AS channel,
  IFNULL(normalized_country_code, "??") AS country,
  EXTRACT(YEAR FROM first_seen.first_seen_date) AS first_seen_year,
  CASE
    WHEN LOWER(distribution_id) = "mozillaonline"
      THEN CONCAT("Firefox Desktop", " ", distribution_id)
    ELSE "Firefox Desktop"
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
  COALESCE(
    `mozfun.norm.windows_version_info`(os, os_version, windows_build_number),
    os_version
  ) AS os_version_build,
  CAST(
    `mozfun.norm.extract_version`(
      COALESCE(
        `mozfun.norm.windows_version_info`(os, os_version, windows_build_number),
        os_version
      ),
      "major"
    ) AS INTEGER
  ) AS os_version_major,
  CAST(
    `mozfun.norm.extract_version`(
      COALESCE(
        `mozfun.norm.windows_version_info`(os, os_version, windows_build_number),
        os_version
      ),
      "minor"
    ) AS INTEGER
  ) AS os_version_minor,
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
  `moz-fx-data-shared-prod.firefox_desktop.usage_reporting_clients_last_seen`
LEFT JOIN
  first_seen
  USING (usage_profile_id)
WHERE
  submission_date >= '2025-03-01'
