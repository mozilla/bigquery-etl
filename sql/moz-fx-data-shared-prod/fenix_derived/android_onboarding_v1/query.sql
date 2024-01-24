-- extract the relevant fields for each funnel step and segment if necessary
WITH onboarding_funnel_new_profile AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    ac.client_id AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_first_card_impression AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '1'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'impression'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_first_card_primary_click AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '1'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'click'
        AND `mozfun.map.get_key`(event_extra, 'element_type') = 'primary_button'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_first_card_secondary_click AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '1'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'click'
        AND `mozfun.map.get_key`(event_extra, 'element_type') = 'secondary_button'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_second_card_impression AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '2'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'impression'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_second_card_primary_click AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '2'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'click'
        AND `mozfun.map.get_key`(event_extra, 'element_type') = 'primary_button'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_second_card_secondary_click AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '2'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'click'
        AND `mozfun.map.get_key`(event_extra, 'element_type') = 'secondary_button'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_third_card_impression AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '3'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'impression'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_third_card_primary_click AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '3'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'click'
        AND `mozfun.map.get_key`(event_extra, 'element_type') = 'primary_button'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_third_card_secondary_click AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'sequence_position') = '3'
        AND `mozfun.map.get_key`(event_extra, 'action') = 'click'
        AND `mozfun.map.get_key`(event_extra, 'element_type') = 'secondary_button'
        AND event_name != 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_onboarding_completed AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN event_name = 'completed'
        AND event_category = 'onboarding'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_sync_sign_in AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN event_name IN ('sign_in', 'sign_up')
        AND event_category = 'sync_auth'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
onboarding_funnel_default_browser AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
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
    COALESCE(r.repeat_first_month_user, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_2, FALSE) AS retained_week_2,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ac.submission_date AS submission_date,
    ac.client_id AS client_id,
    CASE
      WHEN event_name = 'default_browser_changed'
        AND event_category = 'events'
        THEN ac.client_id
    END AS column
  FROM
    fenix.firefox_android_clients ac
  LEFT JOIN
    `moz-fx-data-shared-prod`.fenix_derived.funnel_retention_clients_week_4_v1 r
    ON ac.client_id = r.client_id
  LEFT JOIN
    (SELECT * FROM fenix.events_unnested eu WHERE DATE(submission_timestamp) = @submission_date) eu
    ON ac.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id
      FROM
        fenix.events_unnested
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ac.client_id = funnel_ids.client_id
  WHERE
    ac.submission_date = @submission_date
),
-- aggregate each funnel step value
onboarding_funnel_new_profile_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_new_profile
  GROUP BY
    funnel_id,
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
onboarding_funnel_first_card_impression_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_first_card_impression
  GROUP BY
    funnel_id,
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
onboarding_funnel_first_card_primary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_first_card_primary_click
  GROUP BY
    funnel_id,
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
onboarding_funnel_first_card_secondary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_first_card_secondary_click
  GROUP BY
    funnel_id,
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
onboarding_funnel_second_card_impression_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_second_card_impression
  GROUP BY
    funnel_id,
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
onboarding_funnel_second_card_primary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_second_card_primary_click
  GROUP BY
    funnel_id,
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
onboarding_funnel_second_card_secondary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_second_card_secondary_click
  GROUP BY
    funnel_id,
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
onboarding_funnel_third_card_impression_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_third_card_impression
  GROUP BY
    funnel_id,
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
onboarding_funnel_third_card_primary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_third_card_primary_click
  GROUP BY
    funnel_id,
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
onboarding_funnel_third_card_secondary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_third_card_secondary_click
  GROUP BY
    funnel_id,
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
onboarding_funnel_sync_sign_in_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_sync_sign_in
  GROUP BY
    funnel_id,
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
onboarding_funnel_default_browser_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
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
    onboarding_funnel_default_browser
  GROUP BY
    funnel_id,
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
    COALESCE(onboarding_funnel_new_profile_aggregated.funnel_id) AS funnel_id,
    COALESCE(
      onboarding_funnel_new_profile_aggregated.repeat_first_month_user
    ) AS repeat_first_month_user,
    COALESCE(onboarding_funnel_new_profile_aggregated.retained_week_2) AS retained_week_2,
    COALESCE(onboarding_funnel_new_profile_aggregated.retained_week_4) AS retained_week_4,
    COALESCE(onboarding_funnel_new_profile_aggregated.country) AS country,
    COALESCE(onboarding_funnel_new_profile_aggregated.locale) AS locale,
    COALESCE(onboarding_funnel_new_profile_aggregated.android_version) AS android_version,
    COALESCE(onboarding_funnel_new_profile_aggregated.channel) AS channel,
    COALESCE(onboarding_funnel_new_profile_aggregated.device_model) AS device_model,
    COALESCE(onboarding_funnel_new_profile_aggregated.device_manufacturer) AS device_manufacturer,
    COALESCE(onboarding_funnel_new_profile_aggregated.first_seen_date) AS first_seen_date,
    COALESCE(onboarding_funnel_new_profile_aggregated.adjust_network) AS adjust_network,
    COALESCE(onboarding_funnel_new_profile_aggregated.adjust_campaign) AS adjust_campaign,
    COALESCE(onboarding_funnel_new_profile_aggregated.adjust_creative) AS adjust_creative,
    COALESCE(onboarding_funnel_new_profile_aggregated.adjust_ad_group) AS adjust_ad_group,
    COALESCE(onboarding_funnel_new_profile_aggregated.install_source) AS install_source,
    submission_date,
    funnel,
    COALESCE(onboarding_funnel_new_profile_aggregated.aggregated) AS new_profile,
    COALESCE(
      onboarding_funnel_first_card_impression_aggregated.aggregated
    ) AS first_card_impression,
    COALESCE(
      onboarding_funnel_first_card_primary_click_aggregated.aggregated
    ) AS first_card_primary_click,
    COALESCE(
      onboarding_funnel_first_card_secondary_click_aggregated.aggregated
    ) AS first_card_secondary_click,
    COALESCE(
      onboarding_funnel_second_card_impression_aggregated.aggregated
    ) AS second_card_impression,
    COALESCE(
      onboarding_funnel_second_card_primary_click_aggregated.aggregated
    ) AS second_card_primary_click,
    COALESCE(
      onboarding_funnel_second_card_secondary_click_aggregated.aggregated
    ) AS second_card_secondary_click,
    COALESCE(
      onboarding_funnel_third_card_impression_aggregated.aggregated
    ) AS third_card_impression,
    COALESCE(
      onboarding_funnel_third_card_primary_click_aggregated.aggregated
    ) AS third_card_primary_click,
    COALESCE(
      onboarding_funnel_third_card_secondary_click_aggregated.aggregated
    ) AS third_card_secondary_click,
    COALESCE(onboarding_funnel_onboarding_completed_aggregated.aggregated) AS onboarding_completed,
    COALESCE(onboarding_funnel_sync_sign_in_aggregated.aggregated) AS sync_sign_in,
    COALESCE(onboarding_funnel_default_browser_aggregated.aggregated) AS default_browser,
  FROM
    onboarding_funnel_new_profile_aggregated
  FULL OUTER JOIN
    onboarding_funnel_first_card_impression_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_first_card_primary_click_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_first_card_secondary_click_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_second_card_impression_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_second_card_primary_click_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_second_card_secondary_click_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_third_card_impression_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_third_card_primary_click_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_third_card_secondary_click_aggregated
    USING (
      submission_date,
      funnel_id,
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
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_sync_sign_in_aggregated
    USING (
      submission_date,
      funnel_id,
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
    onboarding_funnel_default_browser_aggregated
    USING (
      submission_date,
      funnel_id,
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
