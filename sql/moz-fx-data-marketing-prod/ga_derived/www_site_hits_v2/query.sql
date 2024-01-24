WITH get_session_start_time AS (
  SELECT
    PARSE_DATE('%Y%m%d', a.event_date) AS date,
    a.user_pseudo_id || '-' || CAST(
      (
        SELECT
          `value`
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ).int_value AS string
    ) AS visit_identifier,
    a.user_pseudo_id AS full_visitor_id,
    MIN(event_timestamp) AS visit_start_time
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    1,
    2,
    3
),
get_all_events_in_each_session_staging AS (
  SELECT
    PARSE_DATE('%Y%m%d', a.event_date) AS date,
    a.user_pseudo_id || '-' || CAST(
      (
        SELECT
          `value`
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ).int_value AS string
    ) AS visit_identifier,
    a.user_pseudo_id AS full_visitor_id,
    a.event_name,
    a.event_timestamp,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.language AS language,
    device.web_info.browser AS browser,
    device.web_info.browser_version AS browser_version,
    geo.country AS country,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS ad_content,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'engagement_time_msec'
      LIMIT
        1
    ).int_value AS engagement_time_msec, --not sure if this is the same as hit time yet or not
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
    COALESCE(
      (SELECT `value` FROM UNNEST(event_params) WHERE key = 'entrances' LIMIT 1).int_value,
      0
    ) AS is_entrance
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
),
--if there are multiple events at the same timestamp in the same session, assign them all the same hit number
get_all_events_in_each_session AS (
  SELECT
    a.*,
    DENSE_RANK() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) AS hit_number,
    ROW_NUMBER() OVER (
      PARTITION BY
        visit_identifier
      ORDER BY
        event_timestamp
    ) AS row_nbr
  FROM
    get_all_events_in_each_session_staging a
),
--get the hit number associated with the last page view in each session
hit_nbr_of_last_page_view_in_each_session AS (
  SELECT
    visit_identifier,
    MAX(hit_number) AS max_hit_number
  FROM
    get_all_events_in_each_session
  WHERE
    event_name = 'page_view'
  GROUP BY
    1
),
--because sometimes there are multiple page views with the same event timestamp,
--choose only 1 to represent the exit, so as not to double count exits
session_exits AS (
  SELECT
    a.visit_identifier,
    b.max_hit_number,
    1 AS is_exit,
    a.event_name,
    MAX(a.row_nbr) AS row_nbr_to_count_as_exit
  FROM
    get_all_events_in_each_session a
  JOIN
    hit_nbr_of_last_page_view_in_each_session b
    ON a.visit_identifier = b.visit_identifier
    AND a.hit_number = b.max_hit_number
    AND a.event_name = 'page_view'
  GROUP BY
    1,
    2,
    3,
    4
)
SELECT
  a.date,
  a.visit_identifier,
  a.full_visitor_id,
  a.visit_start_time,
  b.page_location AS page_path,
  SPLIT(REGEXP_REPLACE(b.page_location, 'https://www.mozilla.org', ''), '/')[
    SAFE_OFFSET(1)
  ] AS page_path_level1,
  CASE
    WHEN b.event_name = 'page_view'
      THEN 'PAGE'
    ELSE 'EVENT'
  END AS hit_type,
  coalesce(c.is_exit,0) AS is_exit,
  b.is_entrance,
  b.hit_number,
  b.event_timestamp AS hit_timestamp,
  NULL AS event_category, --GA4 has no notion of event_label, unlike GA3 (UA360)
  b.event_name,
  NULL AS event_label, --GA4 has no notion of event_label, unlike GA3 (UA360)
  NULL AS event_action, --GA4 has no notion of event_action, unlike GA3 (UA360)
  b.device_category,
  b.operating_system,
  b.language,
  b.browser,
  b.browser_version,
  b.country,
  b.source,
  b.medium,
  b.campaign,
  b.ad_content,
/*
? AS visits,
? AS bounces,
*/
  SAFE_DIVIDE(engagement_time_msec, 1000) AS hit_time,
/*
? AS first_interaction,
? AS last_interaction,
*/
b.is_entrance AS entrances,
coalesce(c.is_exit,0) AS exits,
/*
? AS event_id,
*/
  SPLIT(REGEXP_REPLACE(b.page_location, 'https://www.mozilla.org', ''), '/')[
    SAFE_OFFSET(1)
  ] AS page_level_1,
  SPLIT(REGEXP_REPLACE(b.page_location, 'https://www.mozilla.org', ''), '/')[
    SAFE_OFFSET(2)
  ] AS page_level_2,
  SPLIT(REGEXP_REPLACE(b.page_location, 'https://www.mozilla.org', ''), '/')[
    SAFE_OFFSET(3)
  ] AS page_level_3,
  SPLIT(REGEXP_REPLACE(b.page_location, 'https://www.mozilla.org', ''), '/')[
    SAFE_OFFSET(4)
  ] AS page_level_4,
  SPLIT(REGEXP_REPLACE(b.page_location, 'https://www.mozilla.org', ''), '/')[
    SAFE_OFFSET(5)
  ] AS page_level_5
  /*
  --try to remove query string from full page path
? AS page_name
*/
FROM
  get_session_start_time a
LEFT OUTER JOIN
  get_all_events_in_each_session b
  ON a.date = b.date
  AND a.visit_identifier = b.visit_identifier
  AND a.full_visitor_id = b.full_visitor_id
LEFT OUTER JOIN
  session_exits c
  ON b.visit_identifier = c.visit_identifier
  AND b.row_nbr = c.row_nbr_to_count_as_exit
