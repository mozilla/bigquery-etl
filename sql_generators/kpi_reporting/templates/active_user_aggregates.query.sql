{{ header }}
SELECT
  submission_date,
  app_name,
  normalized_channel,
  first_seen_year,
  is_new_profile,
  country,
  locale,
  -- normalized_os,
  os_version_major,
  os_version_minor,
  os_version_patch,
  app_version,
  distribution_id,
  segment,
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
  -- SUM(uri_count) AS uri_count,
  -- SUM(active_hours_sum) AS active_hours,
FROM
    `{{ project_id }}.{{ dataset }}.reporting_users`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  app_name,
  normalized_channel,
  first_seen_year,
  is_new_profile,
  country,
  locale,
  os_version_major,
  os_version_minor,
  os_version_patch,
  app_version,
  distribution_id,
  segment
