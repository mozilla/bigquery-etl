-- Get all clients who had activity in the last 28 days and their bit patterns.
WITH submission_date_activity AS (
  SELECT
    client_id,
    days_seen_bits,
    submission_date AS activity_date
  FROM
    telemetry.clients_last_seen_v1 -- this might cause an issue because definition of first_seen_date in this table is different from clients_first_seen_v2
  WHERE
    submission_date = DATE("2023-10-01")
  GROUP BY
    client_id,
    submission_date,
    days_seen_bits
),
-- Get all the cohorts that are still in range of the current day of activity (180 days)
cohorts_in_range AS (
  SELECT
    client_id,
    first_seen_date,
    -- DATE(@activity_date) AS activity_date,
    DATE("2023-10-01") AS activity_date,
    -- activity_segment, -- for desktop: from clients_last_seen, for mobile: calculated field from mobile_with_searches in unified_metrics
    app_version,
    attribution_campaign,
    attribution_content,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_ua,
    -- attribution_variation, -- can find in clients_daily_v6 with unpacking attribution field
    partner_distribution_version,
    partner_distributor,
    partner_distributor_channel,
    partner_id,
    city,
    country,
    -- device_model, -- important for mobile, usually null, may not need
    apple_model_id, -- from clients_first_seen_v2
    distribution_id,
    -- is_default_browser,  -- can find in clients_daily_v6
    locale,
    -- normalized_app_name,  -- strings that come from unified_metrics/filters out BrowserStack
    app_name as normalized_app_name, -- from clients_first_seen_v2, as per Lucia's msg app_name = normalized_app_name
    normalized_channel,
    normalized_os,
    normalized_os_version,
    -- os_version_major,
    -- os_version_minor,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
  FROM
    -- telemetry_derived.rolling_cohorts_v1
    telemetry_derived.clients_first_seen_v2
  WHERE
    -- cohort_date
    first_seen_date
    BETWEEN DATE_SUB(DATE("2023-10-01"), INTERVAL 180 DAY)
    AND DATE_SUB(DATE("2023-10-01"), INTERVAL 1 DAY)
    -- (1) No need to get activity for the cohort created on activity_date - everyone will be retained
    -- (2) Note this is a pretty big scan... Look here for problems
),
activity_cohort_match AS (
  SELECT
    cohorts_in_range.client_id AS client_id,
    submission_date_activity.client_id AS active_client_id,
    submission_date_activity.days_seen_bits as active_client_days_seen_bits,
    mozfun.bits28.days_since_seen(submission_date_activity.days_seen_bits) as active_clients_days_since_seen,
    cohorts_in_range.* EXCEPT (client_id)
  FROM
    cohorts_in_range
  LEFT JOIN
    submission_date_activity
  USING
    (client_id, activity_date)
)
SELECT
  first_seen_date,
  activity_date,
  -- activity_segment,
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_ua,
  -- attribution_variation,
  partner_distribution_version,
  partner_distributor,
  partner_distributor_channel,
  partner_id,
  city,
  country,
  -- device_model,
  apple_model_id, -- from clients_first_seen_v2
  distribution_id,
  -- is_default_browser,
  locale,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  os_version_major,
  os_version_minor,
  COUNT(client_id) AS num_clients_in_cohort,
  COUNTIF((active_client_id IS NOT NULL) AND (active_clients_days_since_seen = 0)) AS num_clients_active_on_day,
  COUNTIF((active_client_id IS NOT NULL) AND
    (COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & active_client_days_seen_bits) > 0,
    FALSE
    )) ) AS num_clients_active_atleastonce_in_last_7_days,
  COUNTIF((active_client_id IS NOT NULL) AND
    ( COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & active_client_days_seen_bits) > 0,
    FALSE) )) AS num_clients_active_atleastonce_in_last_28_days,
FROM
  activity_cohort_match
GROUP BY
  first_seen_date,
  activity_date,
  -- activity_segment,
  app_version,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_ua,
  -- attribution_variation,
  partner_distribution_version,
  partner_distributor,
  partner_distributor_channel,
  partner_id,
  city,
  country,
  -- device_model,
  apple_model_id,
  distribution_id,
  -- is_default_browser,
  locale,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  os_version_major,
  os_version_minor
