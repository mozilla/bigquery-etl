{{ header }}

--- Query generated via sql_generators.active_users.
WITH todays_metrics AS (
  SELECT
    submission_date,

    usage_profile_id,
    normalized_channel AS channel,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
    COALESCE(
      `mozfun.norm.windows_version_info`(os, normalized_os_version, windows_build_number),
      normalized_os_version
    ) AS os_version,
    COALESCE(
      CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
    distribution_id,
    is_active,
    activity_segment AS segment,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau

-- -- TODO: verify if these fields are needed
--     app_name,
--     app_version AS app_version,
--     IFNULL(country, '??') country,
--     city,
--     os,
--     COALESCE(
--       scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
--       scalar_parent_browser_engagement_total_uri_count_sum
--     ) AS uri_count,
--     is_default_browser,

  FROM
    `{{ usage_reporting_active_users_view }}`
  WHERE
    submission_date = @submission_date
)
SELECT
  todays_metrics.* EXCEPT (
    usage_profile_id,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau,
    is_active
  ),
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau
FROM
  todays_metrics
GROUP BY
  ALL
