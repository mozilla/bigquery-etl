WITH submission_date_activity AS (
  SELECT
    client_id,
    submission_date AS activity_date
  FROM
    telemetry_derived.unified_metrics_v1
  WHERE
    submission_date = @submission_date
    AND days_since_seen = 0
  GROUP BY
    client_id,
    submission_date
),
-- Get all the cohorts that are still in range of the current day of activity (180 days)
cohorts_in_range AS (
  SELECT
    client_id,
    cohort_date,
    DATE(@submission_date) AS activity_date,
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
  FROM
    telemetry_derived.rolling_cohorts_v1
  WHERE
    cohort_date > DATE_SUB(
      @submission_date,
      INTERVAL 180 DAY
    ) -- Note this is a pretty big scan... Look here for problems
),
activity_cohort_match AS (
  SELECT
    cohorts_in_range.client_id AS cohort_client_id,
    submission_date_activity.client_id AS active_client_id,
    cohorts_in_range.*
  FROM
    cohorts_in_range
  LEFT JOIN
    submission_date_activity
  USING
    (client_id, activity_date)
)
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
  COUNT(cohort_client_id) AS num_clients_in_cohort,
  COUNT(active_client_id) AS num_clients_active_on_day,
FROM
  activity_cohort_match
GROUP BY
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
  os_version_minor
