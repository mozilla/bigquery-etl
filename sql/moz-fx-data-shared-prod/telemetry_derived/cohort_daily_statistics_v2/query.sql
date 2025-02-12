WITH submission_date_activity AS (
  SELECT DISTINCT
    client_id,
    submission_date AS activity_date
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau IS TRUE
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
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    play_store_attribution_content,
    play_store_attribution_term,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v2`
  WHERE
    cohort_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 180 DAY)
    AND DATE_SUB(@submission_date, INTERVAL 1 DAY)
),
activity_cohort_match AS (
  SELECT
    cohorts_in_range.client_id AS cohort_client_id,
    submission_date_activity.client_id AS active_client_id,
    cohorts_in_range.* EXCEPT (client_id)
  FROM
    cohorts_in_range
  LEFT JOIN
    submission_date_activity
    USING (client_id, activity_date)
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
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_content,
  play_store_attribution_term,
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
  os_version_minor,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  play_store_attribution_campaign,
  play_store_attribution_medium,
  play_store_attribution_source,
  play_store_attribution_content,
  play_store_attribution_term
