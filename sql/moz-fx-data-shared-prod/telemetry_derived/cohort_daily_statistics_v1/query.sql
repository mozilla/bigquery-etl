WITH submission_date_activity AS (
  SELECT
    client_id,  -- TODO: Ensure single client
    submission_date AS activity_date
  FROM
    telemetry_derived.unified_metrics_v1
  WHERE
    submission_date = @submission_date
),
-- For every day, get all the cohorts that are still in range (180 days)
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
    cohort_date > DATE_SUB(@submission_date, INTERVAL 180 DAY)  -- TODO: This is a huge scan...
),
activity_cohort_match AS (
    SELECT
        cohorts_in_range.client_id as cohort_client_id,
        submission_date_activity.client_id as active_client_id,
        cohorts_in_range.*
    FROM
        cohorts_in_range
        LEFT JOIN submission_date_activity USING(client_id, activity_date)
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


-- Cohort Statistics:
-- One row per cohort date per activity date (up to 180 days after the cohort date) i.e. summary of the cohort's activity in that day
-- cohort_date, <client/acquisition dimensions>, activity_date, num_clients_in_cohort, num_clients_active_on_day
--
--
--
--
-- cohort_date = d1
-- 	activity_date = d2
-- 	activity_date = d3
-- 	activity_date = d4
-- 	...
-- 	activity_date-date = d181
--
--
-- cohort_date = d2
-- 	activity_date = d3
-- 	activity_date = d4
-- 	activity_date = d5
-- 	...
-- 	activity_date = d182
--
--
--
-- SELECT
-- 	submission_date,
-- 	country,
-- 	num_active,
-- 	num_cohort,
-- WHERE
-- 	cohort_date = <some date>
