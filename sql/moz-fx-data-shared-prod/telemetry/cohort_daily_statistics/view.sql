CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.cohort_daily_statistics`
AS
SELECT
  cohort_date,
  activity_date,
  activity_segment,
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  city,
  country,
  device_model,
  distribution_id,
  is_default_browser,
  locale,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  os_version_major,
  os_version_minor,
  num_clients_in_cohort,
  num_clients_active_on_day
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_daily_statistics_v1`
WHERE
  normalized_app_name != "Firefox Desktop"
UNION ALL
SELECT
  first_seen_date AS cohort_date,
  submission_date AS activity_date,
  CAST(NULL AS STRING) AS activity_segment,
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  CAST(NULL AS STRING) AS attribution_variation,
  city,
  country,
  CAST(NULL AS STRING) AS device_model,
  distribution_id,
  CAST(NULL AS BOOL) AS is_default_browser,
  locale,
  "Firefox Desktop" AS normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  os_version_major,
  os_version_minor,
  num_clients_in_cohort,
  num_clients_active_on_day
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_cohort_daily_retention_v1`
