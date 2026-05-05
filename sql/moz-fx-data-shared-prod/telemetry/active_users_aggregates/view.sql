CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
AS
SELECT
  segment,
  attribution_medium,
  attribution_source,
  attributed,
  adjust_network,
  install_source,
  city,
  country,
  distribution_id,
  first_seen_year,
  is_default_browser,
  channel,
  os,
  os_version,
  os_version_major,
  os_version_minor,
  submission_date,
  locale,
  dau,
  wau,
  mau,
  daily_users,
  weekly_users,
  monthly_users,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
  os_grouped,
  CASE
    WHEN STARTS_WITH(distribution_id, "vivo-")
      THEN "vivo"
    WHEN STARTS_WITH(distribution_id, "dt-")
      THEN "dt"
    ELSE CAST(NULL AS STRING)
  END AS partnership,
FROM
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates_mobile`
UNION ALL
SELECT
  segment_dau AS segment,
  attribution_medium,
  attribution_source,
  attribution_medium IS NOT NULL
  OR attribution_source IS NOT NULL AS attributed,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS install_source,
  city,
  country,
  distribution_id,
  first_seen_year_new AS first_seen_year,
  is_default_browser,
  channel,
  os,
  os_version_build AS os_version,
  os_version_major,
  os_version_minor,
  submission_date,
  locale,
  dau,
  wau,
  mau,
  daily_users,
  weekly_users,
  monthly_users,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
  os_grouped,
  CASE
    WHEN STARTS_WITH(distribution_id, "vivo-")
      THEN "vivo"
    WHEN STARTS_WITH(distribution_id, "dt-")
      THEN "dt"
    ELSE CAST(NULL AS STRING)
  END AS partnership,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.active_users_aggregates`
