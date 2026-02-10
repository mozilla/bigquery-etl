-- extract the relevant fields for each funnel step and segment if necessary
WITH ios_onboarding_funnel_new_profile AS (
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
    ic.client_id AS column
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_first_card_impression AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '1'
        AND event_name = 'card_view'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_first_card_primary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '1'
        AND event_name = 'primary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_first_card_secondary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '1'
        AND event_name = 'secondary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_first_card_close_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '1'
        AND event_name = 'close_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_first_card_multiple_choice_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '1'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_second_card_impression AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '2'
        AND event_name = 'card_view'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_second_card_primary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '2'
        AND event_name = 'primary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_second_card_secondary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '2'
        AND event_name = 'secondary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_second_card_close_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '2'
        AND event_name = 'close_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_second_card_multiple_choice_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '2'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_third_card_impression AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '3'
        AND event_name = 'card_view'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_third_card_primary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '3'
        AND event_name = 'primary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_third_card_secondary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '3'
        AND event_name = 'secondary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_third_card_close_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '3'
        AND event_name = 'close_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_third_card_multiple_choice_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '3'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fourth_card_impression AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '4'
        AND event_name = 'card_view'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fourth_card_primary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '4'
        AND event_name = 'primary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fourth_card_secondary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '4'
        AND event_name = 'secondary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fourth_card_close_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '4'
        AND event_name = 'close_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fourth_card_multiple_choice_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '4'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fifth_card_impression AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '5'
        AND event_name = 'card_view'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fifth_card_primary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '5'
        AND event_name = 'primary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fifth_card_secondary_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '5'
        AND event_name = 'secondary_button_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fifth_card_close_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '5'
        AND event_name = 'close_tap'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_fifth_card_multiple_choice_click AS (
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
      WHEN JSON_VALUE(event_extra.sequence_position) = '5'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_sync_sign_in AS (
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
      WHEN event_name IN ('login_completed_view', 'registration_completed_view')
        AND event_category = 'sync'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
ios_onboarding_funnel_allow_notification AS (
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
      WHEN event_category = 'onboarding'
        AND event_name = 'notification_permission_prompt'
        AND JSON_VALUE(event_extra.granted) = 'true'
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
        `moz-fx-data-shared-prod.firefox_ios.events_stream` eu
      WHERE
        DATE(submission_timestamp) = @submission_date
    ) eu
    USING (client_id)
  LEFT JOIN
    (
      SELECT
        client_id,
        ANY_VALUE(JSON_VALUE(event_extra.sequence_id)) AS funnel_id,
        1 + LENGTH(ANY_VALUE(JSON_VALUE(event_extra.sequence_id))) - LENGTH(
          REPLACE(ANY_VALUE(JSON_VALUE(event_extra.sequence_id)), '_', '')
        ) AS number_of_onboarding_cards
      FROM
        `moz-fx-data-shared-prod.firefox_ios.events_stream`
      WHERE
        JSON_VALUE(event_extra.sequence_id) IS NOT NULL
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        1
    ) funnel_ids
    USING (client_id)
  WHERE
    DATE(ic.submission_timestamp) = @submission_date
),
-- aggregate each funnel step value
ios_onboarding_funnel_new_profile_aggregated AS (
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
    ios_onboarding_funnel_new_profile
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
ios_onboarding_funnel_first_card_impression_aggregated AS (
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
    ios_onboarding_funnel_first_card_impression
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
ios_onboarding_funnel_first_card_primary_click_aggregated AS (
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
    ios_onboarding_funnel_first_card_primary_click
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
ios_onboarding_funnel_first_card_secondary_click_aggregated AS (
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
    ios_onboarding_funnel_first_card_secondary_click
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
ios_onboarding_funnel_first_card_close_click_aggregated AS (
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
    ios_onboarding_funnel_first_card_close_click
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
ios_onboarding_funnel_first_card_multiple_choice_click_aggregated AS (
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
    ios_onboarding_funnel_first_card_multiple_choice_click
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
ios_onboarding_funnel_second_card_impression_aggregated AS (
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
    ios_onboarding_funnel_second_card_impression
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
ios_onboarding_funnel_second_card_primary_click_aggregated AS (
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
    ios_onboarding_funnel_second_card_primary_click
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
ios_onboarding_funnel_second_card_secondary_click_aggregated AS (
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
    ios_onboarding_funnel_second_card_secondary_click
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
ios_onboarding_funnel_second_card_close_click_aggregated AS (
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
    ios_onboarding_funnel_second_card_close_click
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
ios_onboarding_funnel_second_card_multiple_choice_click_aggregated AS (
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
    ios_onboarding_funnel_second_card_multiple_choice_click
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
ios_onboarding_funnel_third_card_impression_aggregated AS (
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
    ios_onboarding_funnel_third_card_impression
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
ios_onboarding_funnel_third_card_primary_click_aggregated AS (
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
    ios_onboarding_funnel_third_card_primary_click
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
ios_onboarding_funnel_third_card_secondary_click_aggregated AS (
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
    ios_onboarding_funnel_third_card_secondary_click
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
ios_onboarding_funnel_third_card_close_click_aggregated AS (
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
    ios_onboarding_funnel_third_card_close_click
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
ios_onboarding_funnel_third_card_multiple_choice_click_aggregated AS (
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
    ios_onboarding_funnel_third_card_multiple_choice_click
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
ios_onboarding_funnel_fourth_card_impression_aggregated AS (
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
    ios_onboarding_funnel_fourth_card_impression
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
ios_onboarding_funnel_fourth_card_primary_click_aggregated AS (
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
    ios_onboarding_funnel_fourth_card_primary_click
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
ios_onboarding_funnel_fourth_card_secondary_click_aggregated AS (
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
    ios_onboarding_funnel_fourth_card_secondary_click
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
ios_onboarding_funnel_fourth_card_close_click_aggregated AS (
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
    ios_onboarding_funnel_fourth_card_close_click
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
ios_onboarding_funnel_fourth_card_multiple_choice_click_aggregated AS (
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
    ios_onboarding_funnel_fourth_card_multiple_choice_click
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
ios_onboarding_funnel_fifth_card_impression_aggregated AS (
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
    ios_onboarding_funnel_fifth_card_impression
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
ios_onboarding_funnel_fifth_card_primary_click_aggregated AS (
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
    ios_onboarding_funnel_fifth_card_primary_click
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
ios_onboarding_funnel_fifth_card_secondary_click_aggregated AS (
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
    ios_onboarding_funnel_fifth_card_secondary_click
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
ios_onboarding_funnel_fifth_card_close_click_aggregated AS (
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
    ios_onboarding_funnel_fifth_card_close_click
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
ios_onboarding_funnel_fifth_card_multiple_choice_click_aggregated AS (
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
    ios_onboarding_funnel_fifth_card_multiple_choice_click
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
ios_onboarding_funnel_sync_sign_in_aggregated AS (
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
    ios_onboarding_funnel_sync_sign_in
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
ios_onboarding_funnel_allow_notification_aggregated AS (
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
    ios_onboarding_funnel_allow_notification
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
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.funnel_id) AS funnel_id,
    COALESCE(
      ios_onboarding_funnel_new_profile_aggregated.repeat_first_month_user
    ) AS repeat_first_month_user,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.retained_week_4) AS retained_week_4,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.country) AS country,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.ios_version) AS ios_version,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.channel) AS channel,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.device_model) AS device_model,
    COALESCE(
      ios_onboarding_funnel_new_profile_aggregated.device_manufacturer
    ) AS device_manufacturer,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.first_seen_date) AS first_seen_date,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.adjust_network) AS adjust_network,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.adjust_campaign) AS adjust_campaign,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.adjust_creative) AS adjust_creative,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.adjust_ad_group) AS adjust_ad_group,
    submission_date,
    funnel,
    COALESCE(ios_onboarding_funnel_new_profile_aggregated.aggregated) AS new_profile,
    COALESCE(
      ios_onboarding_funnel_first_card_impression_aggregated.aggregated
    ) AS first_card_impression,
    COALESCE(
      ios_onboarding_funnel_first_card_primary_click_aggregated.aggregated
    ) AS first_card_primary_click,
    COALESCE(
      ios_onboarding_funnel_first_card_secondary_click_aggregated.aggregated
    ) AS first_card_secondary_click,
    COALESCE(
      ios_onboarding_funnel_first_card_close_click_aggregated.aggregated
    ) AS first_card_close_click,
    COALESCE(
      ios_onboarding_funnel_first_card_multiple_choice_click_aggregated.aggregated
    ) AS first_card_multiple_choice_click,
    COALESCE(
      ios_onboarding_funnel_second_card_impression_aggregated.aggregated
    ) AS second_card_impression,
    COALESCE(
      ios_onboarding_funnel_second_card_primary_click_aggregated.aggregated
    ) AS second_card_primary_click,
    COALESCE(
      ios_onboarding_funnel_second_card_secondary_click_aggregated.aggregated
    ) AS second_card_secondary_click,
    COALESCE(
      ios_onboarding_funnel_second_card_close_click_aggregated.aggregated
    ) AS second_card_close_click,
    COALESCE(
      ios_onboarding_funnel_second_card_multiple_choice_click_aggregated.aggregated
    ) AS second_card_multiple_choice_click,
    COALESCE(
      ios_onboarding_funnel_third_card_impression_aggregated.aggregated
    ) AS third_card_impression,
    COALESCE(
      ios_onboarding_funnel_third_card_primary_click_aggregated.aggregated
    ) AS third_card_primary_click,
    COALESCE(
      ios_onboarding_funnel_third_card_secondary_click_aggregated.aggregated
    ) AS third_card_secondary_click,
    COALESCE(
      ios_onboarding_funnel_third_card_close_click_aggregated.aggregated
    ) AS third_card_close_click,
    COALESCE(
      ios_onboarding_funnel_third_card_multiple_choice_click_aggregated.aggregated
    ) AS third_card_multiple_choice_click,
    COALESCE(
      ios_onboarding_funnel_fourth_card_impression_aggregated.aggregated
    ) AS fourth_card_impression,
    COALESCE(
      ios_onboarding_funnel_fourth_card_primary_click_aggregated.aggregated
    ) AS fourth_card_primary_click,
    COALESCE(
      ios_onboarding_funnel_fourth_card_secondary_click_aggregated.aggregated
    ) AS fourth_card_secondary_click,
    COALESCE(
      ios_onboarding_funnel_fourth_card_close_click_aggregated.aggregated
    ) AS fourth_card_close_click,
    COALESCE(
      ios_onboarding_funnel_fourth_card_multiple_choice_click_aggregated.aggregated
    ) AS fourth_card_multiple_choice_click,
    COALESCE(
      ios_onboarding_funnel_fifth_card_impression_aggregated.aggregated
    ) AS fifth_card_impression,
    COALESCE(
      ios_onboarding_funnel_fifth_card_primary_click_aggregated.aggregated
    ) AS fifth_card_primary_click,
    COALESCE(
      ios_onboarding_funnel_fifth_card_secondary_click_aggregated.aggregated
    ) AS fifth_card_secondary_click,
    COALESCE(
      ios_onboarding_funnel_fifth_card_close_click_aggregated.aggregated
    ) AS fifth_card_close_click,
    COALESCE(
      ios_onboarding_funnel_fifth_card_multiple_choice_click_aggregated.aggregated
    ) AS fifth_card_multiple_choice_click,
    COALESCE(ios_onboarding_funnel_sync_sign_in_aggregated.aggregated) AS sync_sign_in,
    COALESCE(ios_onboarding_funnel_allow_notification_aggregated.aggregated) AS allow_notification,
  FROM
    ios_onboarding_funnel_new_profile_aggregated
  FULL OUTER JOIN
    ios_onboarding_funnel_first_card_impression_aggregated
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
    ios_onboarding_funnel_first_card_primary_click_aggregated
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
    ios_onboarding_funnel_first_card_secondary_click_aggregated
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
    ios_onboarding_funnel_first_card_close_click_aggregated
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
    ios_onboarding_funnel_first_card_multiple_choice_click_aggregated
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
    ios_onboarding_funnel_second_card_impression_aggregated
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
    ios_onboarding_funnel_second_card_primary_click_aggregated
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
    ios_onboarding_funnel_second_card_secondary_click_aggregated
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
    ios_onboarding_funnel_second_card_close_click_aggregated
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
    ios_onboarding_funnel_second_card_multiple_choice_click_aggregated
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
    ios_onboarding_funnel_third_card_impression_aggregated
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
    ios_onboarding_funnel_third_card_primary_click_aggregated
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
    ios_onboarding_funnel_third_card_secondary_click_aggregated
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
    ios_onboarding_funnel_third_card_close_click_aggregated
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
    ios_onboarding_funnel_third_card_multiple_choice_click_aggregated
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
    ios_onboarding_funnel_fourth_card_impression_aggregated
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
    ios_onboarding_funnel_fourth_card_primary_click_aggregated
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
    ios_onboarding_funnel_fourth_card_secondary_click_aggregated
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
    ios_onboarding_funnel_fourth_card_close_click_aggregated
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
    ios_onboarding_funnel_fourth_card_multiple_choice_click_aggregated
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
    ios_onboarding_funnel_fifth_card_impression_aggregated
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
    ios_onboarding_funnel_fifth_card_primary_click_aggregated
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
    ios_onboarding_funnel_fifth_card_secondary_click_aggregated
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
    ios_onboarding_funnel_fifth_card_close_click_aggregated
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
    ios_onboarding_funnel_fifth_card_multiple_choice_click_aggregated
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
    ios_onboarding_funnel_sync_sign_in_aggregated
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
    ios_onboarding_funnel_allow_notification_aggregated
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
