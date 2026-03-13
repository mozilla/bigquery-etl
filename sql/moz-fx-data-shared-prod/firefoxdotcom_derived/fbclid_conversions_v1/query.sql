-- TODO: make sure partition filtering is applied to the queries to reduce the amount of data that needs to be processed.
WITH ga_base AS (
  SELECT
    event_timestamp AS ga_timestamp,
    user_pseudo_id AS ga_client_id,
    event_param.value.string_value AS fbclid,
    geo.country AS ga_country,
  FROM
    `moz-fx-data-marketing-prod.analytics_489412379.events_*`,
    UNNEST(event_params) AS event_param
  WHERE
    _TABLE_SUFFIX
    BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(@activity_date, INTERVAL 9 DAY))
    AND FORMAT_DATE('%Y%m%d', @activity_date)
    AND event_name = "stub_session_set"
    AND user_pseudo_id IS NOT NULL
    AND (event_param.key = 'fbclid' AND event_param.value.string_value IS NOT NULL)
),
ga_fbclid AS (
  SELECT
    ga_timestamp,
    ga_client_id,
    fbclid,
    ga_country,
  FROM
    ga_base
  WHERE
    fbclid IS NOT NULL
),
fbclid_dltoken_mapping AS (
  SELECT
    ga_timestamp,
    ga_client_id,
    ga_dl_token.dl_token,
    fbclid,
    ga_country,
  FROM
    ga_fbclid
  INNER JOIN
    `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1` AS ga_dl_token
    USING (ga_client_id)
  WHERE
    ga_dl_token.first_seen_date
    BETWEEN DATE_SUB(@activity_date, INTERVAL 9 DAY)
    AND @activity_date
),
fbclid_conversions AS (
  SELECT
    fbclid_dltoken_mapping.fbclid,
    fbclid_dltoken_mapping.ga_country,
    conversion_events.report_date AS activity_date,
    conversion_events.client_id,
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
    fbclid_dltoken_mapping
  -- can we do this? Is the expectation that a single dl_token results in a single client?
  INNER JOIN
    `moz-fx-data-shared-prod.google_ads_derived.conversion_event_categorization_v2` AS conversion_events
    ON fbclid_dltoken_mapping.dl_token = conversion_events.attribution_dltoken
  WHERE
    report_date = @activity_date
    AND first_seen_date
    BETWEEN DATE_SUB(@activity_date, INTERVAL 9 DAY)
    AND @activity_date
)
SELECT
  *
FROM
  fbclid_conversions
WHERE
  client_id IS NOT NULL
  AND fbclid IS NOT NULL
  AND (
    first_wk_5_actv_days_and_1_or_more_search_w_ads
    OR first_wk_3_actv_days_and_1_or_more_search_w_ads
    OR first_wk_3_actv_days_and_24_active_minutes
    OR is_dau_at_least_4_of_first_7_days
    OR is_dau_at_least_3_of_first_7_days
    OR is_dau_at_least_2_of_first_7_days
    OR is_dau_at_least_5_of_first_7_days
    OR is_dau_at_least_6_of_first_7_days
    OR is_dau_at_least_7_of_first_7_days
    OR set_default
    OR is_dau_on_days_6_or_7
  )
