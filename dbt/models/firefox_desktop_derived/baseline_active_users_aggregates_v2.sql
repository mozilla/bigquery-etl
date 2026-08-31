-- Ported from bigquery-etl firefox_desktop_derived.baseline_active_users_aggregates_v2.
-- source() replaces the hardcoded table and submission_date_filter() replaces the
-- @submission_date parameter; dbt tracks the source -> model -> downstream lineage.
SELECT
  submission_date,
  first_seen_year,
  channel,
  app_name,
  country,
  city,
  locale,
  os,
  os_grouped,
  os_version,
  os_version_major,
  os_version_minor,
  os_version_build,
  windows_build_number,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
  is_default_browser,
  distribution_id,
  activity_segment,
  attribution_medium,
  attribution_source,
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
  policies_is_enterprise,
FROM
  {{ source('firefox_desktop', 'baseline_active_users') }}
WHERE
  {{ submission_date_filter() }}
GROUP BY
  ALL
