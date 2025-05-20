-- Generated via `usage_reporting` SQL generator.
SELECT
  submission_date,
  first_seen_year,
  channel,
  app_name,
  app_version,
  country,
  os,
  os_version,
  is_default_browser,
  distribution_id,
  activity_segment,
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
FROM
  `moz-fx-data-shared-prod.firefox_ios.usage_reporting_active_users`
WHERE
  submission_date = @submission_date
  AND `date` = @submission_date
GROUP BY
  submission_date,
  first_seen_year,
  channel,
  app_name,
  app_version,
  country,
  os,
  os_version,
  is_default_browser,
  distribution_id,
  activity_segment
