CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.cohort_daily_statistics`
AS
SELECT
  activity_date,
  cohort_date,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  attribution_source,
  attribution_medium,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_variation,
  num_clients_active_on_day,
  num_clients_in_cohort
FROM
  `moz-fx-data-shared-prod.telemetry_derived.cohort_daily_statistics_v1`
WHERE
  normalized_app_name != "Firefox Desktop"
UNION ALL
SELECT
  submission_date AS activity_date,
  first_seen_date AS cohort_date,
  "Firefox Desktop" AS normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  attribution_source,
  attribution_medium,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  CAST(NULL AS STRING) AS attribution_variation,
  num_clients_active_on_day,
  num_clients_in_cohort
FROM
  `moz-fx-data-shared-prod.telemetry_derived.desktop_cohort_daily_retention_v1`