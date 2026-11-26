-- DENG-12035 custom backfill for cohort_daily_statistics_v2: fill is_automated without
-- losing cohorts that rolling_cohorts_v2 has expired.
--
-- Each partition counts cohorts from the previous 768 days, but rolling_cohorts_v2 keeps only
-- 775 days of cohorts. A plain backfill would drop cohorts older than its oldest partition
-- (2024-10-11 on 2026-11-25), up to 91.6M cohort clients per activity date. So:
--   * cohorts before 2026-08-27 are copied from the production partition unchanged, with
--     is_automated = FALSE (no confirmed bot fleet existed before 2026-08-27)
--   * cohorts from 2026-08-27 are recomputed with the logic of query.sql, so is_automated comes
--     from the backfilled rolling_cohorts_v2
-- The columns of `kept` are listed in the same order as the recomputed output, so UNION ALL
-- cannot swap two columns of the same type.
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
-- Get all the cohorts that are still in range of the current day of activity (768 days)
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
    play_store_attribution_install_referrer_response,
    install_source,
    row_source,
    -- Cohorts written before is_automated was added (DENG-12035) have NULL. They are older than
    -- every confirmed bot fleet (the first starts on 2026-08-27), so read NULL as FALSE.
    COALESCE(is_automated, FALSE) AS is_automated,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v2`
  WHERE
    -- Only cohorts from 2026-08-27 can hold confirmed bots. Older cohorts are copied from
    -- production below, because rolling_cohorts_v2 no longer keeps all of them (775-day expiry).
    cohort_date
    BETWEEN GREATEST(DATE_SUB(@submission_date, INTERVAL 768 DAY), DATE '2026-08-27')
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
),
recomputed AS (
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
    row_source,
    COUNT(cohort_client_id) AS num_clients_in_cohort,
    COUNT(active_client_id) AS num_clients_active_on_day,
    play_store_attribution_install_referrer_response,
    install_source,
    is_automated
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
    play_store_attribution_term,
    row_source,
    play_store_attribution_install_referrer_response,
    install_source,
    is_automated
),
kept AS (
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
    row_source,
    num_clients_in_cohort,
    num_clients_active_on_day,
    play_store_attribution_install_referrer_response,
    install_source,
    FALSE AS is_automated
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.cohort_daily_statistics_v2`
  WHERE
    activity_date = @submission_date
    AND cohort_date < DATE '2026-08-27'
)
SELECT
  *
FROM
  kept
UNION ALL
SELECT
  *
FROM
  recomputed
