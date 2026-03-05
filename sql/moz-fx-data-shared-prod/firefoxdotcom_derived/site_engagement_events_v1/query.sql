-- Query for firefoxdotcom_derived.site_engagement_events_v1
-- Flattened GA4 events from firefox.com (excluding What's New Pages)
WITH all_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_name,
    event_timestamp,
    user_pseudo_id AS ga_client_id,
    -- Session ID
    CAST(
      (
        SELECT
          value.int_value
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ) AS STRING
    ) AS ga_session_id,
    (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        key = 'ga_session_number'
      LIMIT
        1
    ) AS ga_session_number,
    (
      SELECT
        IF(x = 1, 1, 0)
      FROM
        (
          SELECT
            COALESCE(value.int_value, SAFE_CAST(value.string_value AS INT64)) AS x
          FROM
            UNNEST(event_params)
          WHERE
            key = 'session_engaged'
          LIMIT
            1
        )
    ) AS session_engaged,
    (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
      LIMIT
        1
    ) AS page_location,
    (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        key = 'engagement_time_msec'
    ) AS engagement_time_msec,
    platform,
    device.category AS device_category,
    device.operating_system AS operating_system,
    geo.country AS geo_country,
    session_traffic_source_last_click.manual_campaign.campaign_id AS campaign_id,
    session_traffic_source_last_click.manual_campaign.campaign_name AS campaign_name,
    session_traffic_source_last_click.manual_campaign.source AS source,
    session_traffic_source_last_click.manual_campaign.medium AS medium,
    session_traffic_source_last_click.manual_campaign.content AS content,
    session_traffic_source_last_click.google_ads_campaign.campaign_name AS google_ads_campaign_name,
    session_traffic_source_last_click.google_ads_campaign.ad_group_name AS google_ads_ad_group_name
  FROM
    `moz-fx-data-marketing-prod.analytics_489412379.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
)
SELECT
  event_date,
  CASE
    WHEN ENDS_WITH(event_name, '_download')
      THEN 'product_download'
    ELSE event_name
  END AS event_name,
  event_timestamp,
  ga_client_id,
  ga_session_id,
  ga_session_number,
  CASE
    WHEN ga_session_number = 1
      THEN TRUE
    ELSE FALSE
  END AS is_new_user,
  session_engaged,
  platform,
  device_category,
  operating_system,
  geo_country,
  campaign_id,
  campaign_name,
  source,
  medium,
  content,
  google_ads_campaign_name,
  google_ads_ad_group_name,
  page_location,
  TRIM(
    SPLIT(REGEXP_REPLACE(page_location, r'^https?://(www\.)?firefox\.com', ''), '/')[
      SAFE_OFFSET(1)
    ],
    '/'
  ) AS page_location_locale,
  REGEXP_REPLACE(
    REGEXP_REPLACE(page_location, r'^https?://(www\.)?firefox\.com/[a-z]{2}(-[A-Z]{2})?', ''),
    r'\?.*$',
    ''
  ) AS page_path,
  engagement_time_msec
FROM
  all_events
WHERE
  page_location IS NOT NULL
  AND LOWER(page_location) NOT LIKE '%/whatsnew/%'
