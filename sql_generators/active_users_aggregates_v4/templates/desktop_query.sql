--- Query generated via sql_generators.active_users.
WITH todays_metrics AS (
  SELECT
    client_id,
    app_name,
    app_version AS app_version,
    normalized_channel AS channel,
    IFNULL(country, '??') AS country,
    IFNULL(city, '??') AS city,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
    os,
    COALESCE(
      `mozfun.norm.windows_version_info`(os, normalized_os_version, windows_build_number),
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
    submission_date,
    is_default_browser,
    distribution_id,
    attribution_source,
    attribution_medium,
    activity_segment AS segment_dau,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year_new,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau
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
    is_mau
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
  app_name,
  app_version,
  channel,
  country,
  city,
  locale,
  os,
  os_version_build,
  os_version_major,
  os_version_minor,
  submission_date,
  is_default_browser,
  distribution_id,
  attribution_source,
  attribution_medium,
  segment_dau,
  first_seen_year_new
