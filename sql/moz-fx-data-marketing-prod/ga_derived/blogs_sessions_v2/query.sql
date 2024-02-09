--Get all page views, the session they belong to, the page location, and a flag for whether it was an entrance or not to the session
WITH all_page_views AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    event_timestamp,
    user_pseudo_id || '-' || CAST(
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
    ) AS visit_identifier,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS `language`,
    geo.country AS country,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS content,
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
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'entrances'
      LIMIT
        1
    ).int_value AS is_entrance
  FROM
    `moz-fx-data-marketing-prod.analytics_314399816.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name = 'page_view'
),
--filter down to entrance pages only, and then filter to only 1 entrance page per session (there should only be 1, but in case for some reason, there is ever more than 1, just pick 1)
entrance_page_views_only AS (
  SELECT
    pg_vws.date,
    pg_vws.visit_identifier,
    pg_vws.device_category,
    pg_vws.operating_system,
    pg_vws.browser,
    pg_vws.language AS `language`,
    pg_vws.country,
    pg_vws.source,
    pg_vws.medium,
    pg_vws.campaign,
    pg_vws.content,
    --need to check below yet, not sure if I have this correct yet
    REGEXP_REPLACE(
      SPLIT(stg.page_location, '?')[SAFE_OFFSET(0)],
      '^https://blog.mozilla.org',
      ''
    ) AS page_path,
    SPLIT(
      REGEXP_REPLACE(
        SPLIT(stg.page_location, '?')[SAFE_OFFSET(0)],
        '^https://blog.mozilla.org',
        ''
      ),
      '/'
    )[SAFE_OFFSET(1)] AS blog,
    SPLIT(
      REGEXP_REPLACE(
        SPLIT(stg.page_location, '?')[SAFE_OFFSET(0)],
        '^https://blog.mozilla.org',
        ''
      ),
      '/'
    )[SAFE_OFFSET(2)] AS subblog
  FROM
    all_page_views pg_vws
  WHERE
    pg_vws.is_entrance = 1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) = 1
)
SELECT
  epvo.date,
  epvo.visit_identifier,
  epvo.device_category,
  epvo.operating_system,
  epvo.browser,
  epvo.language,
  epvo.country,
  epvo.source,
  epvo.medium,
  epvo.campaign,
  epvo.content,
--blog,
--subblog,
--sessions
FROM
  entrance_page_views_only epvo
