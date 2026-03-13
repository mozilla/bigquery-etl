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
    _TABLE_SUFFIX = FORMAT_DATE(
      '%Y%m%d',
      @activity_date
    )  -- TODO: can a single fbclid show up across multiple days? Might prove instrumental when building the fbc value later on. May need to extend this window.
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
-- TODO: we could probably consider just building conversion events table for clients in the future.
events_staging AS (
  SELECT
    client_id,
    MIN(submission_date) AS firefox_first_run_date,
    MIN(
      IF(IFNULL(ad_clicks_count_all, 0) > 0, submission_date, NULL)
    ) AS firefox_first_ad_click_date,
    MIN(IF(IFNULL(search_count_all, 0) > 0, submission_date, NULL)) AS firefox_first_search_date,
    COUNT(DISTINCT(submission_date)) AS nbr_days_running_firefox,
    MAX(submission_date) AS most_recent_date_running_firefox,
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.clients_daily_v6`
  INNER JOIN
    `moz-fx-data-shared-prod.telemetry.clients_first_seen`
    USING (client_id)
  WHERE
    submission_date
    BETWEEN DATE_SUB(@activity_date, INTERVAL 2 DAY)
    AND @activity_date
    AND DATE_DIFF(submission_date, first_seen_date, DAY)
    BETWEEN 0
    AND 2
  GROUP BY
    client_id
),
client_events AS (
  SELECT
    client_id,
    @activity_date AS activity_date,
    COALESCE(@activity_date = firefox_first_run_date, FALSE) AS firefox_first_run,
    COALESCE(@activity_date = firefox_first_ad_click_date, FALSE) AS firefox_first_ad_click,
    COALESCE(@activity_date = firefox_first_search_date, FALSE) AS firefox_first_search,
    COALESCE(
      (@activity_date = most_recent_date_running_firefox)
      AND (nbr_days_running_firefox = 2),
      FALSE
    ) AS returned_second_day,
  FROM
    events_staging
),
fbclid_conversions AS (
  SELECT
    fbclid,
    ga_country,
    ga_event_timestamp,
    activity_date,
    firefox_first_run,
    firefox_first_ad_click,
    firefox_first_search,
    returned_second_day,
  FROM
    fbclid_client_mapping
  INNER JOIN
    client_events
    USING (client_id)
),
current_conversions AS (
  SELECT
    *,
  FROM
    fbclid_conversions UNPIVOT(
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
existing_conversions AS (
  SELECT
    fbclid,
    conversion_name,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_conversion_events_day_2_v1`
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
