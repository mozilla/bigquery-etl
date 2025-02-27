-- extract the relevant fields for each funnel step and segment if necessary
WITH ios_onboarding_funnel_toolbar_bottom AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
    COALESCE(r.repeat_profile, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ic.first_reported_country AS country,
    ic.os_version AS ios_version,
    ic.channel AS channel,
    ic.device_model AS device_model,
    ic.device_manufacturer AS device_manufacturer,
    ic.first_seen_date AS first_seen_date,
    ic.adjust_network AS adjust_network,
    ic.adjust_campaign AS adjust_campaign,
    ic.adjust_creative AS adjust_creative,
    ic.adjust_ad_group AS adjust_ad_group,
    DATE(ic.submission_timestamp) AS submission_date,
    ic.client_id AS client_id_column,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'button_action') = 'toolbar-bottom'
        AND event_name = 'multiple_choice_button_tap'
        AND event_category = 'onboarding'
        THEN ic.client_id
    END AS column
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients` ic --each client_id has only one row
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod`.firefox_ios.retention_clients
      WHERE
        submission_date = @submission_date
        AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
        AND new_profile_metric_date
    ) AS r -- we only new_profile retention
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    ON ic.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id,
        1 + LENGTH(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id'))) - LENGTH(
          REPLACE(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested`
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ic.client_id = funnel_ids.client_id
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_toolbar_top AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
    COALESCE(r.repeat_profile, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ic.first_reported_country AS country,
    ic.os_version AS ios_version,
    ic.channel AS channel,
    ic.device_model AS device_model,
    ic.device_manufacturer AS device_manufacturer,
    ic.first_seen_date AS first_seen_date,
    ic.adjust_network AS adjust_network,
    ic.adjust_campaign AS adjust_campaign,
    ic.adjust_creative AS adjust_creative,
    ic.adjust_ad_group AS adjust_ad_group,
    DATE(ic.submission_timestamp) AS submission_date,
    ic.client_id AS client_id_column,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'button_action') = 'toolbar-top'
        AND event_name = 'multiple_choice_button_tap'
        AND event_category = 'onboarding'
        THEN ic.client_id
    END AS column
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients` ic --each client_id has only one row
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod`.firefox_ios.retention_clients
      WHERE
        submission_date = @submission_date
        AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
        AND new_profile_metric_date
    ) AS r -- we only new_profile retention
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    ON ic.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id,
        1 + LENGTH(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id'))) - LENGTH(
          REPLACE(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested`
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ic.client_id = funnel_ids.client_id
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_theme_dark AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
    COALESCE(r.repeat_profile, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ic.first_reported_country AS country,
    ic.os_version AS ios_version,
    ic.channel AS channel,
    ic.device_model AS device_model,
    ic.device_manufacturer AS device_manufacturer,
    ic.first_seen_date AS first_seen_date,
    ic.adjust_network AS adjust_network,
    ic.adjust_campaign AS adjust_campaign,
    ic.adjust_creative AS adjust_creative,
    ic.adjust_ad_group AS adjust_ad_group,
    DATE(ic.submission_timestamp) AS submission_date,
    ic.client_id AS client_id_column,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'button_action') = 'theme-dark'
        AND event_name = 'multiple_choice_button_tap'
        AND event_category = 'onboarding'
        THEN ic.client_id
    END AS column
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients` ic --each client_id has only one row
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod`.firefox_ios.retention_clients
      WHERE
        submission_date = @submission_date
        AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
        AND new_profile_metric_date
    ) AS r -- we only new_profile retention
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    ON ic.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id,
        1 + LENGTH(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id'))) - LENGTH(
          REPLACE(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested`
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ic.client_id = funnel_ids.client_id
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_theme_light AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
    COALESCE(r.repeat_profile, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ic.first_reported_country AS country,
    ic.os_version AS ios_version,
    ic.channel AS channel,
    ic.device_model AS device_model,
    ic.device_manufacturer AS device_manufacturer,
    ic.first_seen_date AS first_seen_date,
    ic.adjust_network AS adjust_network,
    ic.adjust_campaign AS adjust_campaign,
    ic.adjust_creative AS adjust_creative,
    ic.adjust_ad_group AS adjust_ad_group,
    DATE(ic.submission_timestamp) AS submission_date,
    ic.client_id AS client_id_column,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'button_action') = 'theme-light'
        AND event_name = 'multiple_choice_button_tap'
        AND event_category = 'onboarding'
        THEN ic.client_id
    END AS column
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients` ic --each client_id has only one row
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod`.firefox_ios.retention_clients
      WHERE
        submission_date = @submission_date
        AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
        AND new_profile_metric_date
    ) AS r -- we only new_profile retention
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    ON ic.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id,
        1 + LENGTH(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id'))) - LENGTH(
          REPLACE(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested`
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ic.client_id = funnel_ids.client_id
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_theme_system_auto AS (
  SELECT
    COALESCE(funnel_id, 'no_onboarding_reported') AS funnel_id,
    COALESCE(r.repeat_profile, FALSE) AS repeat_first_month_user,
    COALESCE(r.retained_week_4, FALSE) AS retained_week_4,
    ic.first_reported_country AS country,
    ic.os_version AS ios_version,
    ic.channel AS channel,
    ic.device_model AS device_model,
    ic.device_manufacturer AS device_manufacturer,
    ic.first_seen_date AS first_seen_date,
    ic.adjust_network AS adjust_network,
    ic.adjust_campaign AS adjust_campaign,
    ic.adjust_creative AS adjust_creative,
    ic.adjust_ad_group AS adjust_ad_group,
    DATE(ic.submission_timestamp) AS submission_date,
    ic.client_id AS client_id_column,
    CASE
      WHEN `mozfun.map.get_key`(event_extra, 'button_action') = 'theme-system-default'
        AND event_name = 'multiple_choice_button_tap'
        AND event_category = 'onboarding'
        THEN ic.client_id
    END AS column
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients` ic --each client_id has only one row
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod`.firefox_ios.retention_clients
      WHERE
        submission_date = @submission_date
        AND metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
        AND new_profile_metric_date
    ) AS r -- we only new_profile retention
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        *
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    ON ic.client_id = eu.client_info.client_id
  LEFT JOIN
    (
      SELECT
        client_info.client_id,
        ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')) AS funnel_id,
        1 + LENGTH(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id'))) - LENGTH(
          REPLACE(ANY_VALUE(`mozfun.map.get_key`(event_extra, 'sequence_id')), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_unnested`
      WHERE
        `mozfun.map.get_key`(event_extra, 'sequence_id') IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    ON ic.client_id = funnel_ids.client_id
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
-- aggregate each funnel step value
ios_onboarding_funnel_toolbar_bottom_aggregated AS (
  SELECT
    submission_date,
    "ios_onboarding_funnel" AS funnel,
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    COUNT(DISTINCT column) AS aggregated
  FROM
    ios_onboarding_funnel_toolbar_bottom
  GROUP BY
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    submission_date,
    funnel
),
ios_onboarding_funnel_toolbar_top_aggregated AS (
  SELECT
    submission_date,
    "ios_onboarding_funnel" AS funnel,
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    COUNT(DISTINCT column) AS aggregated
  FROM
    ios_onboarding_funnel_toolbar_top
  GROUP BY
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    submission_date,
    funnel
),
ios_onboarding_funnel_theme_dark_aggregated AS (
  SELECT
    submission_date,
    "ios_onboarding_funnel" AS funnel,
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    COUNT(DISTINCT column) AS aggregated
  FROM
    ios_onboarding_funnel_theme_dark
  GROUP BY
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    submission_date,
    funnel
),
ios_onboarding_funnel_theme_light_aggregated AS (
  SELECT
    submission_date,
    "ios_onboarding_funnel" AS funnel,
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    COUNT(DISTINCT column) AS aggregated
  FROM
    ios_onboarding_funnel_theme_light
  GROUP BY
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    submission_date,
    funnel
),
ios_onboarding_funnel_theme_system_auto_aggregated AS (
  SELECT
    submission_date,
    "ios_onboarding_funnel" AS funnel,
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    COUNT(DISTINCT column) AS aggregated
  FROM
    ios_onboarding_funnel_theme_system_auto
  GROUP BY
    funnel_id,
    repeat_first_month_user,
    retained_week_4,
    country,
    ios_version,
    channel,
    device_model,
    device_manufacturer,
    first_seen_date,
    adjust_network,
    adjust_campaign,
    adjust_creative,
    adjust_ad_group,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.funnel_id) AS funnel_id,
    COALESCE(
      ios_onboarding_funnel_toolbar_bottom_aggregated.repeat_first_month_user
    ) AS repeat_first_month_user,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.retained_week_4) AS retained_week_4,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.country) AS country,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.ios_version) AS ios_version,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.channel) AS channel,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.device_model) AS device_model,
    COALESCE(
      ios_onboarding_funnel_toolbar_bottom_aggregated.device_manufacturer
    ) AS device_manufacturer,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.first_seen_date) AS first_seen_date,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.adjust_network) AS adjust_network,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.adjust_campaign) AS adjust_campaign,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.adjust_creative) AS adjust_creative,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.adjust_ad_group) AS adjust_ad_group,
    submission_date,
    funnel,
    COALESCE(ios_onboarding_funnel_toolbar_bottom_aggregated.aggregated) AS toolbar_bottom,
    COALESCE(ios_onboarding_funnel_toolbar_top_aggregated.aggregated) AS toolbar_top,
    COALESCE(ios_onboarding_funnel_theme_dark_aggregated.aggregated) AS theme_dark,
    COALESCE(ios_onboarding_funnel_theme_light_aggregated.aggregated) AS theme_light,
    COALESCE(ios_onboarding_funnel_theme_system_auto_aggregated.aggregated) AS theme_system_auto,
  FROM
    ios_onboarding_funnel_toolbar_bottom_aggregated
  FULL OUTER JOIN
    ios_onboarding_funnel_toolbar_top_aggregated
    USING (
      submission_date,
      funnel_id,
      repeat_first_month_user,
      retained_week_4,
      country,
      ios_version,
      channel,
      device_model,
      device_manufacturer,
      first_seen_date,
      adjust_network,
      adjust_campaign,
      adjust_creative,
      adjust_ad_group,
      funnel
    )
  FULL OUTER JOIN
    ios_onboarding_funnel_theme_dark_aggregated
    USING (
      submission_date,
      funnel_id,
      repeat_first_month_user,
      retained_week_4,
      country,
      ios_version,
      channel,
      device_model,
      device_manufacturer,
      first_seen_date,
      adjust_network,
      adjust_campaign,
      adjust_creative,
      adjust_ad_group,
      funnel
    )
  FULL OUTER JOIN
    ios_onboarding_funnel_theme_light_aggregated
    USING (
      submission_date,
      funnel_id,
      repeat_first_month_user,
      retained_week_4,
      country,
      ios_version,
      channel,
      device_model,
      device_manufacturer,
      first_seen_date,
      adjust_network,
      adjust_campaign,
      adjust_creative,
      adjust_ad_group,
      funnel
    )
  FULL OUTER JOIN
    ios_onboarding_funnel_theme_system_auto_aggregated
    USING (
      submission_date,
      funnel_id,
      repeat_first_month_user,
      retained_week_4,
      country,
      ios_version,
      channel,
      device_model,
      device_manufacturer,
      first_seen_date,
      adjust_network,
      adjust_campaign,
      adjust_creative,
      adjust_ad_group,
      funnel
    )
)
SELECT
  *
FROM
  merged_funnels
