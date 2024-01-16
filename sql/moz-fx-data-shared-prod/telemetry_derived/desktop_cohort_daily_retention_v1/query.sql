-- Get all clients who had activity for 28 days and get their bit patterns.
WITH submission_date_activity AS (
  SELECT
    client_id,
    days_seen_bits,
    (days_visited_1_uri_bits & days_interacted_bits) AS days_seen_dau_bits,
    DATE(submission_date) as submission_date
  FROM
    telemetry.clients_last_seen_v1
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id,
    submission_date,
    days_seen_bits,
    (days_visited_1_uri_bits & days_interacted_bits)
),
-- Get all the cohorts that are still in range of the current day of activity up to 112 days (which is 4 periods of 28 days)
cohorts_in_range AS (
  SELECT
    client_id,
    first_seen_date,
    DATE(@submission_date) AS submission_date,
    app_version,
    architecture,
    attribution_campaign,
    attribution_content,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_ua,
    partner_distribution_version,
    partner_distributor,
    partner_distributor_channel,
    partner_id,
    platform_version,
    city,
    subdivision1,
    country,
    db_version,
    distribution_id,
    locale,
    app_name as normalized_app_name,
    normalized_channel,
    normalized_os,
    normalized_os_version,
    startup_profile_selection_reason,
    vendor,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
  FROM
    telemetry_derived.clients_first_seen_v2
  WHERE
    first_seen_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 112 DAY)
    AND DATE_SUB(@submission_date, INTERVAL 1 DAY)
),
activity_cohort_match AS (
  SELECT
    cohorts_in_range.client_id AS client_id,
    submission_date_activity.client_id AS active_client_id,
    submission_date_activity.days_seen_bits as active_client_days_seen_bits,
    submission_date_activity.days_seen_dau_bits,
    mozfun.bits28.days_since_seen(submission_date_activity.days_seen_bits) as active_clients_days_since_seen,
    mozfun.bits28.days_since_seen(submission_date_activity.days_seen_dau_bits) as dau_clients_days_since_seen,
    cohorts_in_range.* EXCEPT (client_id)
  FROM
    cohorts_in_range
  LEFT JOIN
    submission_date_activity
    USING (client_id, submission_date)
)
SELECT
  first_seen_date,
  submission_date,
  app_version,
  architecture,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_ua,
  partner_distribution_version,
  partner_distributor,
  partner_distributor_channel,
  partner_id,
  platform_version,
  city,
  subdivision1,
  country,
  db_version,
  distribution_id,
  locale,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  startup_profile_selection_reason,
  vendor,
  os_version_major,
  os_version_minor,
  COUNT(client_id) AS num_clients_in_cohort,
  COUNTIF((active_client_id IS NOT NULL) AND (active_clients_days_since_seen = 0)) AS num_clients_active_on_day,
  COUNTIF((active_client_id IS NOT NULL) AND (dau_clients_days_since_seen = 0)) AS num_clients_dau_on_day,
  COUNTIF((active_client_id IS NOT NULL) AND
    (COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & active_client_days_seen_bits) > 0,
    FALSE
    )) ) AS num_clients_active_atleastonce_in_last_7_days,
  COUNTIF((active_client_id IS NOT NULL) AND
    ( COALESCE(
    BIT_COUNT(active_client_days_seen_bits) > 0,
    FALSE) )) AS num_clients_active_atleastonce_in_last_28_days,
  COUNTIF((active_client_id IS NOT NULL) AND
    DATE_DIFF(submission_date, first_seen_date, DAY) = 27 AND
    ( COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & active_client_days_seen_bits) > 0,
    FALSE) )) AS num_clients_repeat_first_month_users,
  COUNTIF((active_client_id IS NOT NULL) AND
    (COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & days_seen_dau_bits) > 0,
    FALSE
    )) ) AS num_clients_dau_active_atleastonce_in_last_7_days,
  COUNTIF((active_client_id IS NOT NULL) AND
    ( COALESCE(
    BIT_COUNT(days_seen_dau_bits) > 0,
    FALSE) )) AS num_clients_dau_active_atleastonce_in_last_28_days,
  COUNTIF((active_client_id IS NOT NULL) AND
    DATE_DIFF(submission_date, first_seen_date, DAY) = 27 AND
    ( COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & days_seen_dau_bits) > 0,
    FALSE) )) AS num_clients_dau_repeat_first_month_users
FROM
  activity_cohort_match
GROUP BY
  first_seen_date,
  submission_date,
  app_version,
  architecture,
  attribution_campaign,
  attribution_content,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_ua,
  partner_distribution_version,
  partner_distributor,
  partner_distributor_channel,
  partner_id,
  platform_version,
  city,
  subdivision1,
  country,
  db_version,
  distribution_id,
  locale,
  normalized_app_name,
  normalized_channel,
  normalized_os,
  normalized_os_version,
  startup_profile_selection_reason,
  vendor,
  os_version_major,
  os_version_minor
