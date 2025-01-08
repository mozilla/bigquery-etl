-- Query for firefox_desktop_derived.locale_aggregates_v1
WITH todays_metrics AS (
  SELECT
    client_id,
    submission_date,
    app_name,
    app_version,
    normalized_channel AS channel,
    IFNULL(country, '??') AS country,
    city,
    locale,
    os,
    COALESCE(
      `mozfun.norm.windows_version_info`(os, normalized_os_version, windows_build_number),
      normalized_os_version
    ) AS os_version,
    is_default_browser,
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
  submission_date,
  app_name,
  app_version,
  channel,
  country,
  city,
  locale,
  os,
  os_version,
  is_default_browser
