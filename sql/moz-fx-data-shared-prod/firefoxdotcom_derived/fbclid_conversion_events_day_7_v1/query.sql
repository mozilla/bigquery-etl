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
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@activity_date, INTERVAL 9 DAY))
    AND FORMAT_DATE('%Y%m%d', @activity_date)
    AND user_pseudo_id IS NOT NULL
    AND (event_param.key = 'fbclid' AND event_param.value.string_value IS NOT NULL)
),
ga_client_mapping AS (
  SELECT
    ga_client_id,
    client_id,
  FROM
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1` AS attribution_lookup
  INNER JOIN
    `moz-fx-data-shared-prod.telemetry.clients_first_seen` AS first_seen
    ON attribution_lookup.dl_token = first_seen.attribution_dltoken
),
fbclid_client_mapping AS (
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
),
fbclid_conversions AS (
  SELECT
    fbclid_client_mapping.fbclid,
    fbclid_client_mapping.ga_event_timestamp,
    fbclid_client_mapping.ga_country,
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
    fbclid_client_mapping
  INNER JOIN
    `moz-fx-data-shared-prod.google_ads_derived.conversion_event_categorization_v2` AS conversion_events
    USING (client_id)
  WHERE
    report_date = @activity_date
    AND first_seen_date
    BETWEEN DATE_SUB(@activity_date, INTERVAL 9 DAY)
    AND @activity_date
    AND client_id IS NOT NULL
),
current_conversions AS (
  SELECT
    *,
  FROM
    fbclid_conversions UNPIVOT(
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
    AND activity_date = @activity_date
),
existing_conversions AS (
  SELECT
    fbclid,
    conversion_name,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_conversion_events_day_7_v1`
  WHERE
    activity_date < @activity_date
)
SELECT
  * EXCEPT (did_conversion)
FROM
  current_conversions
LEFT JOIN
  existing_conversions
  USING (fbclid, conversion_name)
WHERE
  existing_conversions.fbclid IS NULL
  AND existing_conversions.conversion_name IS NULL
-- Make sure we get the earliest possible entry, the ga_timestamp of the first event with fbclid
-- might prove to be critical when formatting fbclid before sending to Conversions API.
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY fbclid ORDER BY ga_event_timestamp ASC) = 1
