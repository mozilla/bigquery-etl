WITH ga_fbclid AS (
  SELECT
    event_timestamp AS ga_event_timestamp,
    user_pseudo_id AS ga_client_id,
    geo.country AS ga_country,
    event_param.value.string_value AS fbclid,
  FROM
    `moz-fx-data-marketing-prod.analytics_489412379.events_*`,
    UNNEST(event_params) AS event_param
  WHERE
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@submission_date, INTERVAL 14 DAY))
    AND FORMAT_DATE('%Y%m%d', @submission_date)
    AND user_pseudo_id IS NOT NULL
    AND (event_param.key = 'fbclid' AND event_param.value.string_value IS NOT NULL)
  -- We want to get the earliest entry for each fbclid
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY fbclid ORDER BY ga_event_timestamp ASC) = 1
),
clients_first_seen_base AS (
  SELECT
    first_seen_date,
    client_id,
    attribution_dltoken AS dl_token,
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_first_seen`
  WHERE
    first_seen_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 14 DAY)
    AND @submission_date
),
ga_client_mapping AS (
  SELECT
    ga_client_id,
    client_id,
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1`
  INNER JOIN
    clients_first_seen_base
    USING (dl_token)
),
fbclid_clients AS (
  SELECT
    ga_event_timestamp,
    ga_country,
    fbclid,
    client_id,
  FROM
    ga_fbclid
  INNER JOIN
    ga_client_mapping
    USING (ga_client_id)
  WHERE
    client_id IS NOT NULL
),
early_conversion_events AS (
  WITH events_stage AS (
    SELECT
      client_id,
      MIN(first_seen_date) AS first_seen_date,
      MIN(submission_date) AS firefox_first_run_date,
      MIN(
        IF(IFNULL(ad_clicks_count_all, 0) > 0, submission_date, NULL)
      ) AS firefox_first_ad_click_date,
      MIN(IF(IFNULL(search_count_all, 0) > 0, submission_date, NULL)) AS firefox_first_search_date,
      COUNT(DISTINCT(submission_date)) AS nbr_days_running_firefox,
      MAX(submission_date) AS most_recent_date_running_firefox,
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6` AS clients_daily
    INNER JOIN
      clients_first_seen_base
      USING (client_id)
    WHERE
      clients_daily.submission_date
      BETWEEN DATE_SUB(@submission_date, INTERVAL 2 DAY)
      AND @submission_date
    GROUP BY
      client_id
  ),
  client_events AS (
    SELECT
      fbclid,
      ga_event_timestamp,
      ga_country,
      DATE(@submission_date) AS activity_date,
      COALESCE(@submission_date = firefox_first_run_date, FALSE) AS firefox_first_run,
      COALESCE(@submission_date = firefox_first_ad_click_date, FALSE) AS firefox_first_ad_click,
      COALESCE(@submission_date = firefox_first_search_date, FALSE) AS firefox_first_search,
      COALESCE(
        (DATE_SUB(@submission_date, INTERVAL 2 DAY) = first_seen_date)
        AND (nbr_days_running_firefox = 2),
        FALSE
      ) AS returned_second_day
    FROM
      events_stage
    INNER JOIN
      fbclid_clients
      USING (client_id)
  )
  SELECT
    * EXCEPT (did_conversion),
  FROM
    client_events UNPIVOT(
      did_conversion FOR conversion_name IN (
        firefox_first_run,
        firefox_first_ad_click,
        firefox_first_search,
        returned_second_day
      )
    )
  WHERE
    did_conversion
),
activity_conversion_events AS (
  WITH events_stage AS (
    SELECT
      fbclid,
      ga_event_timestamp,
      ga_country,
      conversion_events.report_date AS activity_date,
      conversion_events.event_1 AS first_wk_5_actv_days_and_1_or_more_search_w_ads,
      conversion_events.event_2 AS first_wk_3_actv_days_and_1_or_more_search_w_ads,
      conversion_events.event_3 AS first_wk_3_actv_days_and_24_active_minutes,
      conversion_events.is_dau_at_least_4_of_first_7_days,
      conversion_events.is_dau_at_least_3_of_first_7_days,
      conversion_events.is_dau_at_least_2_of_first_7_days,
      conversion_events.is_dau_at_least_5_of_first_7_days,
      conversion_events.is_dau_at_least_6_of_first_7_days,
      conversion_events.is_dau_at_least_7_of_first_7_days,
      conversion_events.set_default,
      conversion_events.is_dau_on_days_6_or_7,
    FROM
      fbclid_clients
    INNER JOIN
      `moz-fx-data-shared-prod.google_ads_derived.conversion_event_categorization_v2` AS conversion_events
      USING (client_id)
    WHERE
      report_date = @submission_date
      AND first_seen_date
      BETWEEN DATE_SUB(@submission_date, INTERVAL 14 DAY)
      AND @submission_date
  )
  SELECT
    * EXCEPT (did_conversion),
  FROM
    events_stage UNPIVOT(
      did_conversion FOR conversion_name IN (
        first_wk_5_actv_days_and_1_or_more_search_w_ads,
        first_wk_3_actv_days_and_1_or_more_search_w_ads,
        first_wk_3_actv_days_and_24_active_minutes,
        is_dau_at_least_4_of_first_7_days,
        is_dau_at_least_3_of_first_7_days,
        is_dau_at_least_2_of_first_7_days,
        is_dau_at_least_5_of_first_7_days,
        is_dau_at_least_6_of_first_7_days,
        is_dau_at_least_7_of_first_7_days,
        set_default,
        is_dau_on_days_6_or_7
      )
    )
  WHERE
    did_conversion
),
current_conversions AS (
  SELECT
    *
  FROM
    early_conversion_events
  UNION ALL
    BY NAME
  SELECT
    *
  FROM
    activity_conversion_events
),
existing_conversions AS (
  SELECT
    fbclid,
    conversion_name,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_desktop_conversion_events_v1`
  WHERE
    activity_date < @submission_date
)
SELECT
  @submission_date AS submission_date,
  *,
FROM
  current_conversions
LEFT JOIN
  existing_conversions
  USING (fbclid, conversion_name)
WHERE
  existing_conversions.fbclid IS NULL
  AND existing_conversions.conversion_name IS NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY fbclid, conversion_name ORDER BY ga_event_timestamp ASC) = 1
