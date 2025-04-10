WITH raw_events_data AS (
  --raw events data gets every event that has an associated/identified Google Analytics session ID
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS `date`,
    user_pseudo_id || '-' || CAST(e.value.int_value AS string) AS visit_identifier,
    event_timestamp,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.language AS `language`,
    geo.country AS country,
    traffic_source.name AS traffic_source_name,
    traffic_source.medium AS traffic_source_medium,
    traffic_source.source AS traffic_source_source,
    collected_traffic_source.manual_campaign_id AS manual_campaign_id,
    collected_traffic_source.manual_term AS manual_term,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS ad_content,
    device.web_info.browser AS browser,
    event_name,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'campaign'
      LIMIT
        1
    ).string_value AS campaign_from_event_params,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'product'
      LIMIT
        1
    ).string_value AS product_type,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'platform'
      LIMIT
        1
    ).string_value AS platform_type,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'source'
      LIMIT
        1
    ).string_value AS source_from_event_params,
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*`
  JOIN
    UNNEST(event_params) AS e
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND e.key = 'ga_session_id'
    AND e.value.int_value IS NOT NULL
),
--next, we group by a variety of dimensions and count # of downloads by each dimension
event_level_data AS (
  SELECT
    `date`,
    visit_identifier,
    device_category,
    operating_system,
    `language`,
    country,
    traffic_source_name,
    traffic_source_medium,
    traffic_source_source,
    manual_campaign_id,
    manual_term,
    source,
    medium,
    campaign,
    ad_content,
    browser,
    campaign_from_event_params,
    source_from_event_params,
  --note: the 2 columns are the same because in GA4, there is no logic saying you can only count 1 download per session, unlike GA3
    COUNTIF(
    --prior to and including 2/16/24
      (
        `date` <= '2024-02-16'
        AND event_name = 'product_download'
        AND platform_type IN (
          'win',
          'win64',
          'macos',
          'linux64',
          'win64-msi',
          'linux',
          'win-msi',
          'win64-aarch64'
        )
        AND product_type = 'firefox'
      )
      OR
    --on and after 2/17/24
      (`date` >= '2024-02-17' AND event_name = 'firefox_download')
    ) AS download_events
  FROM
    raw_events_data stg
  GROUP BY
    `date`,
    visit_identifier,
    device_category,
    operating_system,
    `language`,
    country,
    traffic_source_name,
    traffic_source_medium,
    traffic_source_source,
    manual_campaign_id,
    manual_term,
    source,
    medium,
    campaign,
    ad_content,
    browser,
    campaign_from_event_params,
    source_from_event_params
),
--next, calculate what was the first campaign seen in the session
first_campaign_from_event_params_in_session AS (
  SELECT
    visit_identifier,
    campaign_from_event_params AS first_campaign_from_event_params_in_session,
    ROW_NUMBER() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) AS rnk
  FROM
    raw_events_data
  WHERE
    campaign_from_event_params IS NOT NULL
  QUALIFY
    rnk = 1
),
--next, calculate what was the first source seen in the session
first_source_from_event_params_in_session AS (
  SELECT
    visit_identifier,
    source_from_event_params AS first_source_from_event_params,
    ROW_NUMBER() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) AS rnk
  FROM
    raw_events_data
  WHERE
    source_from_event_params IS NOT NULL
  QUALIFY
    rnk = 1
)
SELECT
  a.`date`,
  a.visit_identifier,
  a.device_category,
  a.operating_system,
  a.`language`,
  a.country,
  a.traffic_source_name,
  a.traffic_source_medium,
  a.traffic_source_source,
  a.manual_campaign_id,
  a.manual_term,
  a.source,
  a.medium,
  a.campaign,
  a.ad_content,
  a.browser,
  a.campaign_from_event_params,
  b.first_campaign_from_event_params_in_session,
  a.source_from_event_params,
  c.first_source_from_event_params AS first_source_from_event_params_in_session,
  a.download_events,
  a.download_events AS downloads,
  IF(
    NOT `moz-fx-data-shared-prod.udf.ga_is_mozilla_browser`(a.browser),
    a.download_events,
    0
  ) AS non_fx_downloads
FROM
  event_level_data a
LEFT JOIN
  first_campaign_from_event_params_in_session b
  ON a.visit_identifier = b.visit_identifier
LEFT JOIN
  first_source_from_event_params_in_session c
  ON a.visit_identifier = c.visit_identifier
