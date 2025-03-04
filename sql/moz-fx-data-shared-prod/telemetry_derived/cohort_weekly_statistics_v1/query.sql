WITH clients_first_seen AS (
  SELECT
    normalized_app_name,
    normalized_channel,
    app_version,
    attribution_campaign,
    attribution_content,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_variation,
    country,
    device_model,
    distribution_id,
    is_default_browser,
    locale,
    normalized_os,
    normalized_os_version,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    play_store_attribution_content,
    play_store_attribution_term,
    DATE_TRUNC(cohort_date, WEEK) AS cohort_date_week,
    client_id
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v2`
  WHERE
    cohort_date >= DATE_TRUNC(
      DATE_SUB(@submission_date, INTERVAL 180 day),
      WEEK
    ) --start of week for date 180 days ago
    AND LOWER(normalized_app_name) NOT LIKE '%browserstack'
    AND LOWER(normalized_app_name) NOT LIKE '%mozillaonline'
),
submission_date_activity AS (
  SELECT DISTINCT
    client_id,
    submission_date AS activity_date,
    DATE_TRUNC(submission_date, WEEK) AS activity_date_week
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users`
  WHERE
    submission_date > DATE_TRUNC(
      DATE_SUB(@submission_date, INTERVAL 180 day),
      WEEK
    ) --start of week for date 180 days ago
    AND submission_date <= DATE_SUB(
      DATE_TRUNC(@submission_date, WEEK),
      INTERVAL 1 DAY
    ) --through last completed week
    AND is_dau IS TRUE
),
clients_first_seen_in_last_180_days_and_activity_next_180_days AS (
  SELECT
    a.normalized_app_name,
    a.normalized_channel,
    a.app_version,
    a.attribution_campaign,
    a.attribution_content,
    a.attribution_experiment,
    a.attribution_medium,
    a.attribution_source,
    a.attribution_variation,
    a.country,
    a.device_model,
    a.distribution_id,
    a.is_default_browser,
    a.locale,
    a.normalized_os,
    a.normalized_os_version,
    a.adjust_ad_group,
    a.adjust_campaign,
    a.adjust_creative,
    a.adjust_network,
    a.play_store_attribution_campaign,
    a.play_store_attribution_medium,
    a.play_store_attribution_source,
    a.play_store_attribution_content,
    a.play_store_attribution_term,
    a.cohort_date_week,
    b.activity_date_week,
    COUNT(DISTINCT(b.client_id)) AS nbr_active_clients
  FROM
    clients_first_seen a
  LEFT JOIN
    submission_date_activity b
    ON a.client_id = b.client_id
    AND a.cohort_date_week <= b.activity_date_week
  GROUP BY
    a.normalized_app_name,
    a.normalized_channel,
    a.app_version,
    a.attribution_campaign,
    a.attribution_content,
    a.attribution_experiment,
    a.attribution_medium,
    a.attribution_source,
    a.attribution_variation,
    a.country,
    a.device_model,
    a.distribution_id,
    a.is_default_browser,
    a.locale,
    a.normalized_os,
    a.normalized_os_version,
    a.adjust_ad_group,
    a.adjust_campaign,
    a.adjust_creative,
    a.adjust_network,
    a.play_store_attribution_campaign,
    a.play_store_attribution_medium,
    a.play_store_attribution_source,
    a.play_store_attribution_content,
    a.play_store_attribution_term,
    a.cohort_date_week,
    b.activity_date_week
),
--get # of unique clients by cohort start date week, normalized app name, channel, and app version
initial_cohort_counts AS (
  SELECT
    normalized_app_name,
    normalized_channel,
    app_version,
    attribution_campaign,
    attribution_content,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_variation,
    country,
    device_model,
    distribution_id,
    is_default_browser,
    locale,
    normalized_os,
    normalized_os_version,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    play_store_attribution_content,
    play_store_attribution_term,
    cohort_date_week,
    COUNT(DISTINCT(client_id)) AS nbr_clients_in_cohort
  FROM
    clients_first_seen
  GROUP BY
    normalized_app_name,
    normalized_channel,
    app_version,
    attribution_campaign,
    attribution_content,
    attribution_experiment,
    attribution_medium,
    attribution_source,
    attribution_variation,
    country,
    device_model,
    distribution_id,
    is_default_browser,
    locale,
    normalized_os,
    normalized_os_version,
    adjust_ad_group,
    adjust_campaign,
    adjust_creative,
    adjust_network,
    play_store_attribution_campaign,
    play_store_attribution_medium,
    play_store_attribution_source,
    play_store_attribution_content,
    play_store_attribution_term,
    cohort_date_week
)
SELECT
  i.normalized_app_name,
  i.normalized_channel,
  i.app_version,
  i.attribution_campaign,
  i.attribution_content,
  i.attribution_experiment,
  i.attribution_medium,
  i.attribution_source,
  i.attribution_variation,
  i.country,
  i.device_model,
  i.distribution_id,
  i.is_default_browser,
  i.locale,
  i.normalized_os,
  i.normalized_os_version,
  i.adjust_ad_group,
  i.adjust_campaign,
  i.adjust_creative,
  i.adjust_network,
  i.play_store_attribution_campaign,
  i.play_store_attribution_medium,
  i.play_store_attribution_source,
  i.play_store_attribution_content,
  i.play_store_attribution_term,
  i.cohort_date_week,
  i.nbr_clients_in_cohort,
  a.activity_date_week,
  DATE_DIFF(a.activity_date_week, i.cohort_date_week, WEEK) AS weeks_after_first_seen_week,
  a.nbr_active_clients
FROM
  initial_cohort_counts AS i
LEFT JOIN
  clients_first_seen_in_last_180_days_and_activity_next_180_days AS a
  ON COALESCE(i.normalized_app_name, 'NULL') = COALESCE(a.normalized_app_name, 'NULL')
  AND COALESCE(i.normalized_channel, 'NULL') = COALESCE(a.normalized_channel, 'NULL')
  AND COALESCE(i.app_version, 'NULL') = COALESCE(a.app_version, 'NULL')
  AND COALESCE(i.attribution_campaign, 'NULL') = COALESCE(a.attribution_campaign, 'NULL')
  AND COALESCE(i.attribution_content, 'NULL') = COALESCE(a.attribution_content, 'NULL')
  AND COALESCE(i.attribution_experiment, 'NULL') = COALESCE(a.attribution_experiment, 'NULL')
  AND COALESCE(i.attribution_medium, 'NULL') = COALESCE(a.attribution_medium, 'NULL')
  AND COALESCE(i.attribution_source, 'NULL') = COALESCE(a.attribution_source, 'NULL')
  AND COALESCE(i.attribution_variation, 'NULL') = COALESCE(a.attribution_variation, 'NULL')
  AND COALESCE(i.country, 'NULL') = COALESCE(a.country, 'NULL')
  AND COALESCE(i.device_model, 'NULL') = COALESCE(a.device_model, 'NULL')
  AND COALESCE(i.distribution_id, 'NULL') = COALESCE(a.distribution_id, 'NULL')
  AND COALESCE(CAST(i.is_default_browser AS STRING), 'NULL') = COALESCE(
    CAST(a.is_default_browser AS string),
    'NULL'
  )
  AND COALESCE(i.locale, 'NULL') = COALESCE(a.locale, 'NULL')
  AND COALESCE(i.normalized_os, 'NULL') = COALESCE(a.normalized_os, 'NULL')
  AND COALESCE(i.normalized_os_version, 'NULL') = COALESCE(a.normalized_os_version, 'NULL')
  AND COALESCE(i.adjust_ad_group, 'NULL') = COALESCE(a.adjust_ad_group, 'NULL')
  AND COALESCE(i.adjust_campaign, 'NULL') = COALESCE(a.adjust_campaign, 'NULL')
  AND COALESCE(i.adjust_creative, 'NULL') = COALESCE(a.adjust_network, 'NULL')
  AND COALESCE(i.adjust_network, 'NULL') = COALESCE(a.adjust_network, 'NULL')
  AND COALESCE(i.play_store_attribution_campaign, 'NULL') = COALESCE(
    a.play_store_attribution_campaign,
    'NULL'
  )
  AND COALESCE(i.play_store_attribution_medium, 'NULL') = COALESCE(
    a.play_store_attribution_medium,
    'NULL'
  )
  AND COALESCE(i.play_store_attribution_source, 'NULL') = COALESCE(
    a.play_store_attribution_source,
    'NULL'
  )
  AND COALESCE(i.play_store_attribution_content, 'NULL') = COALESCE(
    a.play_store_attribution_content,
    'NULL'
  )
  AND COALESCE(i.play_store_attribution_term, 'NULL') = COALESCE(
    a.play_store_attribution_term,
    'NULL'
  )
  AND i.cohort_date_week = a.cohort_date_week
