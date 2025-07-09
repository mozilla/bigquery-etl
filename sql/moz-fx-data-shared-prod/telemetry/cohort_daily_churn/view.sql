CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.cohort_daily_churn`
AS
--Get all users who were consider a daily active user in the last 180 days
WITH submission_date_activity AS (
  SELECT DISTINCT
    client_id,
    submission_date AS activity_date
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users`
  WHERE
    submission_date >= DATE_SUB(CURRENT_DATE, INTERVAL 180 DAY)
    AND is_dau IS TRUE
),
-- Get all the clients first seen in the last 180 days and attrs as of first seen
cohorts_in_range AS (
  SELECT
    client_id,
    cohort_date, --first seen date
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
    play_store_attribution_install_referrer_response,
    row_source,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v2`
  WHERE
    cohort_date >= DATE_SUB(CURRENT_DATE, INTERVAL 180 DAY)
),
--Get all first seen in last 180 days left joined
--to 1 row per day/client where they were DAU for the last 180 days
activity_cohort_match AS (
  SELECT
    cohorts_in_range.client_id AS cohort_client_id,
    submission_date_activity.client_id AS active_client_id,
    submission_date_activity.activity_date,
    cohorts_in_range.* EXCEPT (client_id)
  FROM
    cohorts_in_range
  LEFT JOIN
    submission_date_activity
    USING (client_id)
)
SELECT
  cohort_date,
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
  play_store_attribution_install_referrer_response,
  row_source,
  COUNT(DISTINCT(cohort_client_id)) AS num_clients_in_cohort,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date = DATE_ADD(cohort_date, INTERVAL 1 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_on_day_1,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date
          BETWEEN DATE_ADD(cohort_date, INTERVAL 1 DAY)
          AND DATE_ADD(cohort_date, INTERVAL 2 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_any_day_between_day_1_and_day_2,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date
          BETWEEN DATE_ADD(cohort_date, INTERVAL 1 DAY)
          AND DATE_ADD(cohort_date, INTERVAL 3 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_any_day_between_day_1_and_day_3,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date
          BETWEEN DATE_ADD(cohort_date, INTERVAL 1 DAY)
          AND DATE_ADD(cohort_date, INTERVAL 4 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_any_day_between_day_1_and_day_4,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date
          BETWEEN DATE_ADD(cohort_date, INTERVAL 1 DAY)
          AND DATE_ADD(cohort_date, INTERVAL 5 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_any_day_between_day_1_and_day_5,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date
          BETWEEN DATE_ADD(cohort_date, INTERVAL 1 DAY)
          AND DATE_ADD(cohort_date, INTERVAL 6 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_any_day_between_day_1_and_day_6,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date
          BETWEEN DATE_ADD(cohort_date, INTERVAL 1 DAY)
          AND DATE_ADD(cohort_date, INTERVAL 7 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_any_day_between_day_1_and_day_7,
  COUNT(
    DISTINCT(
      CASE
        WHEN activity_date
          BETWEEN DATE_ADD(cohort_date, INTERVAL 1 DAY)
          AND DATE_ADD(cohort_date, INTERVAL 28 DAY)
          THEN active_client_id
        ELSE NULL
      END
    )
  ) AS num_clients_returned_any_day_between_day_1_and_day_28
FROM
  activity_cohort_match
GROUP BY
  cohort_date,
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
  play_store_attribution_install_referrer_response,
  row_source
