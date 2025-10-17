--Step 1 - get all page view events on mozilla.org for the submission date
WITH all_page_view_events AS (
  SELECT
    *,
    CAST(
      (
        SELECT
          `value`
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ).int_value AS STRING
    ) AS ga_session_id,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
      LIMIT
        1
    ).string_value AS page_location,
    (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        key = 'engagement_time_msec'
    ) AS engagement_time_msec
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name = 'page_view'
),
--Step 2: Get the subset of events that are page views of the Firefox "whats new" pages
--        Additionally parse out info from the whats new page path
whats_new_page_page_views AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id AS ga_client_id,
    ga_session_id,
    platform,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.operating_system_version,
    geo.country AS geo_country,
    geo.city AS geo_city,
    geo.region AS geo_region,
    geo.sub_continent AS geo_sub_continent,
    geo.metro AS geo_metro,
    device.language AS device_language,
    device.web_info.browser AS device_browser,
    device.web_info.browser_version AS device_browser_version,
    event_params,
  --traffic source = first-ever source that acquired the user_pseudo_id
    traffic_source.name AS traffic_source_name,
    traffic_source.medium AS traffic_source_medium,
    traffic_source.source AS traffic_source_source,
  --collected traffic source = source for that event (can change from event to event)
    collected_traffic_source.manual_campaign_id AS cts_manual_campaign_id,
    collected_traffic_source.manual_campaign_name AS cts_manual_campaign_name,
    collected_traffic_source.manual_source AS cts_manual_source,
    collected_traffic_source.manual_medium AS cts_manual_medium,
    collected_traffic_source.manual_term AS cts_manual_term,
    collected_traffic_source.manual_content AS cts_manual_content,
    collected_traffic_source.manual_source_platform AS cts_manual_source_platform,
    collected_traffic_source.manual_creative_format AS cts_manual_creative_format,
    collected_traffic_source.manual_marketing_tactic AS cts_manual_marketing_tactic,
  --the last source before starting their session
    session_traffic_source_last_click.manual_campaign.campaign_id AS session_src_manual_campaign_id,
    session_traffic_source_last_click.manual_campaign.campaign_name AS session_src_manual_campaign_name,
    session_traffic_source_last_click.manual_campaign.source AS session_src_manual_campaign_source,
    session_traffic_source_last_click.manual_campaign.medium AS sesssion_src_manual_campaign_medium,
    session_traffic_source_last_click.manual_campaign.term AS session_src_manual_campaign_term,
    session_traffic_source_last_click.manual_campaign.content AS session_src_manual_campaign_content,
    session_traffic_source_last_click.google_ads_campaign.customer_id AS session_src_customer_id,
    session_traffic_source_last_click.google_ads_campaign.account_name AS session_src_account_name,
    session_traffic_source_last_click.google_ads_campaign.campaign_id AS session_src_campaign_id,
    session_traffic_source_last_click.google_ads_campaign.campaign_name AS session_src_campaign_name,
    session_traffic_source_last_click.google_ads_campaign.ad_group_id AS session_src_ad_group_id,
    session_traffic_source_last_click.google_ads_campaign.ad_group_name AS session_src_ad_group_name,
    page_location,
    TRIM(
      SPLIT(REGEXP_REPLACE(page_location, '^https://www.mozilla.org', ''), '/')[SAFE_OFFSET(1)],
      '/'
    ) AS page_location_locale,
    SPLIT(REGEXP_REPLACE(page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(3)
    ] AS page_level_2,
    REGEXP_EXTRACT(page_location, r'[?&]oldversion=([^&]+)') AS oldversion,
    REGEXP_EXTRACT(page_location, r'[?&]newversion=([^&]+)') AS newversion,
    engagement_time_msec
  FROM
    all_page_view_events
  WHERE
    LOWER(page_location) LIKE '%whatsnew%'
    AND LOWER(
      SPLIT(REGEXP_REPLACE(page_location, '^https://www.mozilla.org', ''), '/')[SAFE_OFFSET(2)]
    ) = 'firefox'
)
SELECT
  wnp.*,
  mozfun.norm.browser_version_info(wnp.page_level_2) AS page_level_2_version_info,
  --handling for weird edge cases that break version parsing
  CASE
    WHEN wnp.oldversion LIKE '999999999999999999999999%'
      THEN mozfun.norm.browser_version_info(NULL)
    ELSE mozfun.norm.browser_version_info(wnp.oldversion)
  END AS old_version_version_info,
  mozfun.norm.browser_version_info(wnp.newversion) AS new_version_version_info
FROM
  whats_new_page_page_views wnp
