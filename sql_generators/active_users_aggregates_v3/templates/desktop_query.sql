--- Query generated via sql_generators.active_users.
WITH todays_metrics AS (
  SELECT
    client_id,
    activity_segment AS segment,
    app_name,
    app_version AS app_version,
    normalized_channel AS channel,
    IFNULL(country, '??') country,
    IFNULL(city, '??') city,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
    os,
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
    submission_date,
    COALESCE(
      scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
      scalar_parent_browser_engagement_total_uri_count_sum
    ) AS uri_count,
    is_default_browser,
    distribution_id,
    attribution_source,
    attribution_medium,
    attribution_medium IS NOT NULL
    OR attribution_source IS NOT NULL AS attributed,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau,
    active_hours_sum
  FROM
    `moz-fx-data-shared-prod.telemetry.desktop_active_users`
  WHERE
    submission_date = @submission_date
)
SELECT
  todays_metrics.* EXCEPT (
    client_id,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau,
    uri_count,
    active_hours_sum
  ),
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
  SUM(uri_count) AS uri_count,
  SUM(active_hours_sum) AS active_hours,
FROM
  todays_metrics
GROUP BY
  segment,
  app_name,
  app_version,
  channel,
  country,
  city,
  locale,
  first_seen_year,
  os,
  os_version,
  os_version_major,
  os_version_minor,
  submission_date,
  is_default_browser,
  distribution_id,
  attribution_source,
  attribution_medium,
  attributed
