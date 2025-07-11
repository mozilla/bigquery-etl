--- User-facing view for all mobile apps. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates_mobile`
AS
SELECT
  segment,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  locale,
  adjust_network,
  install_source,
  os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.fenix.active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
UNION ALL
SELECT
  segment,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  locale,
  adjust_network,
  install_source,
  os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.firefox_ios.active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
UNION ALL
SELECT
  segment,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  locale,
  adjust_network,
  install_source,
  os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.focus_ios.active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  AND submission_date < "2025-03-27"
  -- As per DENG-8914, we want to use composite tables for focus products
  -- these fields do not exist in the composite view.
UNION ALL
SELECT
  activity_segment AS segment,
  CAST(NULL AS STRING) AS attribution_medium,
  CAST(NULL AS STRING) AS attribution_source,
  CAST(NULL AS BOOLEAN) AS attributed,
  CAST(NULL AS STRING) AS city,
  CAST(NULL AS STRING) AS locale,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS install_source,
  `mozfun.norm.os`(os) AS os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.focus_ios.composite_active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  AND submission_date >= "2025-03-27"
UNION ALL
SELECT
  segment,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  locale,
  adjust_network,
  install_source,
  os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.klar_ios.active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
UNION ALL
SELECT
  segment,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  locale,
  adjust_network,
  install_source,
  os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.focus_android.active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  AND submission_date < "2025-03-27"
  -- As per DENG-8914, we want to use composite tables for focus products
  -- these fields do not exist in the composite view.
UNION ALL
SELECT
  activity_segment AS segment,
  CAST(NULL AS STRING) AS attribution_medium,
  CAST(NULL AS STRING) AS attribution_source,
  CAST(NULL AS BOOLEAN) AS attributed,
  CAST(NULL AS STRING) AS city,
  CAST(NULL AS STRING) AS locale,
  CAST(NULL AS STRING) AS adjust_network,
  CAST(NULL AS STRING) AS install_source,
  `mozfun.norm.os`(os) AS os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.focus_android.composite_active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  AND submission_date >= "2025-03-27"
UNION ALL
SELECT
  segment,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  locale,
  adjust_network,
  install_source,
  os_grouped,
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
  daily_users,
  weekly_users,
  monthly_users,
  dau,
  wau,
  mau,
  app_name,
  app_version,
  app_version_major,
  app_version_minor,
  app_version_patch_revision,
  app_version_is_major_release,
FROM
  `moz-fx-data-shared-prod.klar_android.active_users_aggregates`
WHERE
      -- Hard filter to introduce two day lag to match legacy desktop telemetry KPI delay
      -- in order to avoid confusion.
  submission_date < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
