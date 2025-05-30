CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users_aggregates`
AS
--Note: This view has reduced dimensions compared to the base table
--in order to reduce cardinality in Looker and improve performance
--for full dimensions, please see alternate view baseline_active_users_aggregates_full
SELECT
  submission_date,
  first_seen_year,
  channel,
  app_name,
  country,
  locale,
  os_grouped,
  os_version_major,
  os_version_minor,
  os_version_build,
  windows_build_number,
  app_version,
  app_version_major,
  app_version_is_major_release,
  is_default_browser,
  distribution_id,
  activity_segment,
  attribution_medium,
  attribution_source,
  `mozfun.norm.glean_windows_version_info`(
    os_grouped,
    os_version,
    windows_build_number
  ) AS windows_version,
  SUM(daily_users) AS daily_users,
  SUM(weekly_users) AS weekly_users,
  SUM(monthly_users) AS monthly_users,
  SUM(dau) AS dau,
  SUM(wau) AS wau,
  SUM(mau) AS mau,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_active_users_aggregates_v2`
GROUP BY
  submission_date,
  first_seen_year,
  channel,
  app_name,
  country,
  locale,
  os_grouped,
  os_version_major,
  os_version_minor,
  os_version_build,
  windows_build_number,
  app_version,
  app_version_major,
  app_version_is_major_release,
  is_default_browser,
  distribution_id,
  activity_segment,
  attribution_medium,
  attribution_source,
  `mozfun.norm.glean_windows_version_info`(os_grouped, os_version, windows_build_number)
