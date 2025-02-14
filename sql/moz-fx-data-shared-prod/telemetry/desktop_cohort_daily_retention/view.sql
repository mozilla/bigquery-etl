CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_cohort_daily_retention`
AS
SELECT
  cohort_date AS first_seen_date,
  activity_date AS submission_date,
  activity_segment,
  app_version,
  CAST(NULL AS STRING) AS architecture, --not in new cohort_daily_statistics_v2 yet
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_variation,
  CAST(NULL AS STRING) AS attribution_ua,  --not in new cohort_daily_statistics_v2 yet
  city,
  CAST(NULL AS STRING) AS subdivision_1, --not in new cohort_daily_statistics_v2 yet
  country,
  CAST(NULL AS STRING) AS db_version, --not in new cohort_daily_statistics_v2 yet
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
  num_clients_active_on_day,
  CAST(NULL AS INTEGER) AS num_clients_active_atleastonce_in_last_7_days, --NEED TO ADD STILL
  CAST(NULL AS INTEGER) AS num_clients_active_atleastonce_in_last_28_days, --NEED TO ADD STILL
  CAST(NULL AS INTEGER) AS num_clients_repeat_first_month_users,  --NEED TO ADD STILL
  CAST(NULL AS INTEGER) AS num_clients_dau_active_atleastonce_in_last_7_days,  --NEED TO ADD STILL
  CAST(NULL AS INTEGER) AS num_clients_dau_active_atleastonce_in_last_28_days,  --NEED TO ADD STILL
  CAST(NULL AS INTEGER) AS num_clients_dau_repeat_first_month_users,  --NEED TO ADD STILL
  num_clients_active_on_day AS num_clients_dau_on_day
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_daily_statistics_v2`
WHERE
  normalized_app_name IN ('Firefox Desktop', 'Firefox Desktop MozillaOnline')
  AND activity_date <= current_date --view requires a filter on activity_date
