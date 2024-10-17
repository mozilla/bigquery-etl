{{ header }}
SELECT
  submission_date,
  client_id,
  app_name,
  normalized_channel,
  EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
  submission_date = first_seen_date AS is_new_profile,
  IFNULL(country, '??') country,
  city,
  COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
  normalized_os,
  -- normalized_os_version,
  COALESCE(
    SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
    0
  ) AS os_version_major,
  COALESCE(
    SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
    0
  ) AS os_version_minor,
  COALESCE(
    SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER),
    0
  ) AS os_version_patch,
  app_display_version AS app_version,
  device_model,
  distribution_id,
  activity_segment AS segment,
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
  client_id,
  app_name,
  normalized_channel,
  first_seen_year,
  is_new_profile,
  country,
  city,
  locale,
  normalized_os,
  os_version_major,
  os_version_minor,
  os_version_patch,
  app_version,
  device_model,
  distribution_id,
  segment
