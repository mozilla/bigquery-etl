-- extract the relevant fields for each funnel step and segment if necessary
WITH onboarding_funnel_first_card AS (
  SELECT
    client_info.client_id AS join_key,
    `mozfun.map.get_key`(event_extra, 'sequence_id') AS funnel_id,
    `mozfun.map.get_key`(event_extra, 'action') AS action,
    `mozfun.map.get_key`(event_extra, 'element_type') AS element_type,
    ac.first_reported_country AS country,
    ac.locale AS locale,
    ac.os_version AS android_version,
    ac.channel AS channel,
    ac.device_model AS device_model,
    ac.device_manufacturer AS device_manufacturer,
    ac.first_seen_date AS first_seen_date,
    ac.adjust_network AS adjust_network,
    ac.adjust_campaign AS adjust_campaign,
    ac.adjust_creative AS adjust_creative,
    ac.adjust_ad_group AS adjust_ad_group,
    ac.install_source AS install_source,
    r.repeat_first_month_user AS repeat_first_month_user,
    r.retained_week_2 AS retained_week_2,
    r.retained_week_4 AS retained_week_4,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested eu
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
  ON
    eu.client_info.client_id = r.client_id
  LEFT JOIN
    fenix.firefox_android_clients ac
  ON
    eu.client_info.client_id = ac.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND `mozfun.map.get_key`(event_extra, 'sequence_position') = '1'
    AND event_name != 'completed'
    AND event_category = 'onboarding'
),
onboarding_funnel_second_card AS (
  SELECT
    client_info.client_id AS join_key,
    prev.funnel_id AS funnel_id,
    prev.action AS action,
    prev.element_type AS element_type,
    prev.country AS country,
    prev.locale AS locale,
    prev.android_version AS android_version,
    prev.channel AS channel,
    prev.device_model AS device_model,
    prev.device_manufacturer AS device_manufacturer,
    prev.first_seen_date AS first_seen_date,
    prev.adjust_network AS adjust_network,
    prev.adjust_campaign AS adjust_campaign,
    prev.adjust_creative AS adjust_creative,
    prev.adjust_ad_group AS adjust_ad_group,
    prev.install_source AS install_source,
    prev.repeat_first_month_user AS repeat_first_month_user,
    prev.retained_week_2 AS retained_week_2,
    prev.retained_week_4 AS retained_week_4,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested eu
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
  ON
    eu.client_info.client_id = r.client_id
  LEFT JOIN
    fenix.firefox_android_clients ac
  ON
    eu.client_info.client_id = ac.client_id
  INNER JOIN
    onboarding_funnel_first_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND `mozfun.map.get_key`(event_extra, 'sequence_position') = '2'
    AND event_name != 'completed'
    AND event_category = 'onboarding'
),
onboarding_funnel_third_card AS (
  SELECT
    client_info.client_id AS join_key,
    prev.funnel_id AS funnel_id,
    prev.action AS action,
    prev.element_type AS element_type,
    prev.country AS country,
    prev.locale AS locale,
    prev.android_version AS android_version,
    prev.channel AS channel,
    prev.device_model AS device_model,
    prev.device_manufacturer AS device_manufacturer,
    prev.first_seen_date AS first_seen_date,
    prev.adjust_network AS adjust_network,
    prev.adjust_campaign AS adjust_campaign,
    prev.adjust_creative AS adjust_creative,
    prev.adjust_ad_group AS adjust_ad_group,
    prev.install_source AS install_source,
    prev.repeat_first_month_user AS repeat_first_month_user,
    prev.retained_week_2 AS retained_week_2,
    prev.retained_week_4 AS retained_week_4,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested eu
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
  ON
    eu.client_info.client_id = r.client_id
  LEFT JOIN
    fenix.firefox_android_clients ac
  ON
    eu.client_info.client_id = ac.client_id
  INNER JOIN
    onboarding_funnel_second_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND `mozfun.map.get_key`(event_extra, 'sequence_position') = '3'
    AND event_name != 'completed'
    AND event_category = 'onboarding'
),
onboarding_funnel_onboarding_completed AS (
  SELECT
    client_info.client_id AS join_key,
    prev.funnel_id AS funnel_id,
    prev.action AS action,
    prev.element_type AS element_type,
    prev.country AS country,
    prev.locale AS locale,
    prev.android_version AS android_version,
    prev.channel AS channel,
    prev.device_model AS device_model,
    prev.device_manufacturer AS device_manufacturer,
    prev.first_seen_date AS first_seen_date,
    prev.adjust_network AS adjust_network,
    prev.adjust_campaign AS adjust_campaign,
    prev.adjust_creative AS adjust_creative,
    prev.adjust_ad_group AS adjust_ad_group,
    prev.install_source AS install_source,
    prev.repeat_first_month_user AS repeat_first_month_user,
    prev.retained_week_2 AS retained_week_2,
    prev.retained_week_4 AS retained_week_4,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested eu
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
  ON
    eu.client_info.client_id = r.client_id
  LEFT JOIN
    fenix.firefox_android_clients ac
  ON
    eu.client_info.client_id = ac.client_id
  INNER JOIN
    onboarding_funnel_third_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'completed'
    AND event_category = 'onboarding'
),
-- aggregate each funnel step value
onboarding_funnel_first_card_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_first_card
  GROUP BY
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    submission_date,
    funnel
),
onboarding_funnel_second_card_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_second_card
  GROUP BY
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    submission_date,
    funnel
),
onboarding_funnel_third_card_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_third_card
  GROUP BY
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    submission_date,
    funnel
),
onboarding_funnel_onboarding_completed_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_onboarding_completed
  GROUP BY
    funnel_id,
    action,
    element_type,
    country,
    locale,
    android_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    install_source,
    repeat_first_month_user,
    retained_week_2,
    retained_week_4,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(onboarding_funnel_first_card_aggregated.action) AS action,
    COALESCE(onboarding_funnel_first_card_aggregated.funnel_id) AS funnel_id,
    COALESCE(onboarding_funnel_first_card_aggregated.element_type) AS element_type,
    COALESCE(
      onboarding_funnel_first_card_aggregated.repeat_first_month_user
    ) AS repeat_first_month_user,
    COALESCE(onboarding_funnel_first_card_aggregated.retained_week_2) AS retained_week_2,
    COALESCE(onboarding_funnel_first_card_aggregated.retained_week_4) AS retained_week_4,
    COALESCE(onboarding_funnel_first_card_aggregated.country) AS country,
    COALESCE(onboarding_funnel_first_card_aggregated.locale) AS locale,
    COALESCE(onboarding_funnel_first_card_aggregated.android_version) AS android_version,
    COALESCE(onboarding_funnel_first_card_aggregated.channel) AS channel,
    COALESCE(onboarding_funnel_first_card_aggregated.device_model) AS device_model,
    COALESCE(onboarding_funnel_first_card_aggregated.device_manufacturer) AS device_manufacturer,
    COALESCE(onboarding_funnel_first_card_aggregated.first_seen_date) AS first_seen_date,
    COALESCE(onboarding_funnel_first_card_aggregated.adjust_network) AS adjust_network,
    COALESCE(onboarding_funnel_first_card_aggregated.adjust_campaign) AS adjust_campaign,
    COALESCE(onboarding_funnel_first_card_aggregated.adjust_creative) AS adjust_creative,
    COALESCE(onboarding_funnel_first_card_aggregated.adjust_ad_group) AS adjust_ad_group,
    COALESCE(onboarding_funnel_first_card_aggregated.install_source) AS install_source,
    submission_date,
    funnel,
    COALESCE(onboarding_funnel_first_card_aggregated.aggregated) AS first_card,
    COALESCE(onboarding_funnel_second_card_aggregated.aggregated) AS second_card,
    COALESCE(onboarding_funnel_third_card_aggregated.aggregated) AS third_card,
    COALESCE(onboarding_funnel_onboarding_completed_aggregated.aggregated) AS onboarding_completed,
  FROM
    onboarding_funnel_first_card_aggregated
  FULL OUTER JOIN
    onboarding_funnel_second_card_aggregated
  USING
    (
      submission_date,
      action,
      funnel_id,
      element_type,
      repeat_first_month_user,
      retained_week_2,
      retained_week_4,
      country,
      locale,
      android_version,
      channel,
      device_model,
      device_manufacturer,
      first_seen_date,
      adjust_network,
      adjust_campaign,
      adjust_creative,
      adjust_ad_group,
      install_source,
      funnel
    )
  FULL OUTER JOIN
    onboarding_funnel_third_card_aggregated
  USING
    (
      submission_date,
      action,
      funnel_id,
      element_type,
      repeat_first_month_user,
      retained_week_2,
      retained_week_4,
      country,
      locale,
      android_version,
      channel,
      device_model,
      device_manufacturer,
      first_seen_date,
      adjust_network,
      adjust_campaign,
      adjust_creative,
      adjust_ad_group,
      install_source,
      funnel
    )
  FULL OUTER JOIN
    onboarding_funnel_onboarding_completed_aggregated
  USING
    (
      submission_date,
      action,
      funnel_id,
      element_type,
      repeat_first_month_user,
      retained_week_2,
      retained_week_4,
      country,
      locale,
      android_version,
      channel,
      device_model,
      device_manufacturer,
      first_seen_date,
      adjust_network,
      adjust_campaign,
      adjust_creative,
      adjust_ad_group,
      install_source,
      funnel
    )
)
SELECT
  *
FROM
  merged_funnels
