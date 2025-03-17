WITH clients_first_seen AS (
  --Get 1 row per client ID, with all their attributes as of their first seen date
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
    AND cohort_date <= DATE_SUB(
      DATE_TRUNC(@submission_date, WEEK),
      INTERVAL 1 DAY
    ) --end of last completed week
    AND LOWER(normalized_app_name) NOT LIKE '%browserstack'
    AND LOWER(normalized_app_name) NOT LIKE '%mozillaonline'
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id
      ORDER BY
        cohort_date ASC
    ) = 1  --necessary due to mobile having some clients with multiple first seen dates
),
weekly_active_clients AS (
  --Get 1 row per client ID & week where the client ID was a DAU at least 1 day during that week
  SELECT DISTINCT
    client_id,
    DATE_TRUNC(submission_date, WEEK) AS activity_date_week
  FROM
    `moz-fx-data-shared-prod.telemetry.active_users`
  WHERE
    submission_date >= DATE_TRUNC(
      DATE_SUB(@submission_date, INTERVAL 180 day),
      WEEK
    ) --start of week for date 180 days ago
    AND submission_date <= DATE_SUB(
      DATE_TRUNC(@submission_date, WEEK),
      INTERVAL 1 DAY
    ) --through end of last completed week
    AND is_dau IS TRUE
),
unique_weeks AS (
  SELECT DISTINCT
    first_date_of_week AS activity_date_week
  FROM
    `mozdata.external.calendar`
  WHERE
    submission_date >= DATE_TRUNC(
      DATE_SUB(@submission_date, INTERVAL 180 day),
      WEEK
    ) --start of week 180 days ago
    AND submission_date <= DATE_SUB(
      DATE_TRUNC(@submission_date, WEEK),
      INTERVAL 1 DAY
    ) --end of last completed week
),
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
),
unique_week_group_combos AS (
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
    w.activity_date_week
  FROM
    initial_cohort_counts i
  CROSS JOIN
    unique_weeks w
),
weekly_active_agg AS (
  SELECT
    cfs.normalized_app_name,
    cfs.normalized_channel,
    cfs.app_version,
    cfs.attribution_campaign,
    cfs.attribution_content,
    cfs.attribution_experiment,
    cfs.attribution_medium,
    cfs.attribution_source,
    cfs.attribution_variation,
    cfs.country,
    cfs.device_model,
    cfs.distribution_id,
    cfs.is_default_browser,
    cfs.locale,
    cfs.normalized_os,
    cfs.normalized_os_version,
    cfs.adjust_ad_group,
    cfs.adjust_campaign,
    cfs.adjust_creative,
    cfs.adjust_network,
    cfs.play_store_attribution_campaign,
    cfs.play_store_attribution_medium,
    cfs.play_store_attribution_source,
    cfs.play_store_attribution_content,
    cfs.play_store_attribution_term,
    cfs.cohort_date_week,
    wac.activity_date_week,
    COUNT(DISTINCT(wac.client_id)) AS nbr_active_clients
  FROM
    clients_first_seen cfs
  JOIN
    weekly_active_clients wac
    ON cfs.client_id = wac.client_id
    AND cfs.cohort_date_week <= wac.activity_date_week
  GROUP BY
    cfs.normalized_app_name,
    cfs.normalized_channel,
    cfs.app_version,
    cfs.attribution_campaign,
    cfs.attribution_content,
    cfs.attribution_experiment,
    cfs.attribution_medium,
    cfs.attribution_source,
    cfs.attribution_variation,
    cfs.country,
    cfs.device_model,
    cfs.distribution_id,
    cfs.is_default_browser,
    cfs.locale,
    cfs.normalized_os,
    cfs.normalized_os_version,
    cfs.adjust_ad_group,
    cfs.adjust_campaign,
    cfs.adjust_creative,
    cfs.adjust_network,
    cfs.play_store_attribution_campaign,
    cfs.play_store_attribution_medium,
    cfs.play_store_attribution_source,
    cfs.play_store_attribution_content,
    cfs.play_store_attribution_term,
    cfs.cohort_date_week,
    wac.activity_date_week
)
SELECT
  uwgc.normalized_app_name,
  uwgc.normalized_channel,
  uwgc.app_version,
  uwgc.attribution_campaign,
  uwgc.attribution_content,
  uwgc.attribution_experiment,
  uwgc.attribution_medium,
  uwgc.attribution_source,
  uwgc.attribution_variation,
  uwgc.country,
  uwgc.device_model,
  uwgc.distribution_id,
  uwgc.is_default_browser,
  uwgc.locale,
  uwgc.normalized_os,
  uwgc.normalized_os_version,
  uwgc.adjust_ad_group,
  uwgc.adjust_campaign,
  uwgc.adjust_creative,
  uwgc.adjust_network,
  uwgc.play_store_attribution_campaign,
  uwgc.play_store_attribution_medium,
  uwgc.play_store_attribution_source,
  uwgc.play_store_attribution_content,
  uwgc.play_store_attribution_term,
  uwgc.cohort_date_week,
  uwgc.nbr_clients_in_cohort,
  uwgc.activity_date_week,
  DATE_DIFF(uwgc.activity_date_week, uwgc.cohort_date_week, WEEK) AS weeks_after_first_seen_week,
  COALESCE(waa.nbr_active_clients, 0) AS nbr_active_clients
FROM
  unique_week_group_combos uwgc
LEFT JOIN
  weekly_active_agg waa
  ON COALESCE(uwgc.normalized_app_name, 'NULL') = COALESCE(waa.normalized_app_name, 'NULL')
  AND COALESCE(uwgc.normalized_channel, 'NULL') = COALESCE(waa.normalized_channel, 'NULL')
  AND COALESCE(uwgc.app_version, 'NULL') = COALESCE(waa.app_version, 'NULL')
  AND COALESCE(uwgc.attribution_campaign, 'NULL') = COALESCE(waa.attribution_campaign, 'NULL')
  AND COALESCE(uwgc.attribution_content, 'NULL') = COALESCE(waa.attribution_content, 'NULL')
  AND COALESCE(uwgc.attribution_experiment, 'NULL') = COALESCE(waa.attribution_experiment, 'NULL')
  AND COALESCE(uwgc.attribution_medium, 'NULL') = COALESCE(waa.attribution_medium, 'NULL')
  AND COALESCE(uwgc.attribution_source, 'NULL') = COALESCE(waa.attribution_source, 'NULL')
  AND COALESCE(uwgc.attribution_variation, 'NULL') = COALESCE(waa.attribution_variation, 'NULL')
  AND COALESCE(uwgc.country, 'NULL') = COALESCE(waa.country, 'NULL')
  AND COALESCE(uwgc.device_model, 'NULL') = COALESCE(waa.device_model, 'NULL')
  AND COALESCE(uwgc.distribution_id, 'NULL') = COALESCE(waa.distribution_id, 'NULL')
  AND COALESCE(CAST(uwgc.is_default_browser AS string), 'NULL') = COALESCE(
    CAST(waa.is_default_browser AS string),
    'NULL'
  )
  AND COALESCE(uwgc.locale, 'NULL') = COALESCE(waa.locale, 'NULL')
  AND COALESCE(uwgc.normalized_os, 'NULL') = COALESCE(waa.normalized_os, 'NULL')
  AND COALESCE(uwgc.normalized_os_version, 'NULL') = COALESCE(waa.normalized_os_version, 'NULL')
  AND COALESCE(uwgc.adjust_ad_group, 'NULL') = COALESCE(waa.adjust_ad_group, 'NULL')
  AND COALESCE(uwgc.adjust_campaign, 'NULL') = COALESCE(waa.adjust_campaign, 'NULL')
  AND COALESCE(uwgc.adjust_creative, 'NULL') = COALESCE(waa.adjust_creative, 'NULL')
  AND COALESCE(uwgc.adjust_network, 'NULL') = COALESCE(waa.adjust_network, 'NULL')
  AND COALESCE(uwgc.play_store_attribution_campaign, 'NULL') = COALESCE(
    waa.play_store_attribution_campaign,
    'NULL'
  )
  AND COALESCE(uwgc.play_store_attribution_medium, 'NULL') = COALESCE(
    waa.play_store_attribution_medium,
    'NULL'
  )
  AND COALESCE(uwgc.play_store_attribution_source, 'NULL') = COALESCE(
    waa.play_store_attribution_source,
    'NULL'
  )
  AND COALESCE(uwgc.play_store_attribution_content, 'NULL') = COALESCE(
    waa.play_store_attribution_content,
    'NULL'
  )
  AND COALESCE(uwgc.play_store_attribution_term, 'NULL') = COALESCE(
    waa.play_store_attribution_term,
    'NULL'
  )
  AND uwgc.cohort_date_week = waa.cohort_date_week
  AND uwgc.activity_date_week = waa.activity_date_week
WHERE
  uwgc.activity_date_week >= uwgc.cohort_date_week
