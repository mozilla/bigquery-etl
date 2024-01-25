-- Query for ga_derived.www_site_hits_v2
-- For more information on writing queries see:
-- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH get_session_start_time AS (
  SELECT
    SAFE.PARSE_DATE('%Y%m%d', a.event_date) AS date,
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
      ).int_value AS STRING
    ) AS visit_identifier,
    a.user_pseudo_id AS full_visitor_id,
    MIN(event_timestamp) AS visit_start_time
  FROM
    `moz-fx-data-marketing-prod.analytics_313696158.events_*` a
  WHERE
    _TABLE_SUFFIX = SAFE.FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    date,
    visit_identifier,
    full_visitor_id
),
get_all_events_in_each_session_staging AS (
  SELECT
    SAFE.PARSE_DATE('%Y%m%d', a.event_date) AS date,
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
        key = 'engaged_session_event'
      LIMIT
        1
    ).int_value AS engaged_session_event,
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
    _TABLE_SUFFIX = SAFE.FORMAT_DATE('%Y%m%d', @submission_date)
),
--if there are multiple events at the same timestamp in the same session, assign them all the same hit number
get_all_events_in_each_session AS (
  SELECT
    a.date,
    a.visit_identifier,
    a.full_visitor_id,
    a.event_name,
    a.event_timestamp,
    a.device_category,
    a.operating_system,
    a.language,
    a.browser,
    a.browser_version,
    a.country,
    a.source,
    a.medium,
    a.campaign,
    a.ad_content,
    a.engagement_time_msec,
    a.engaged_session_event,
    SPLIT(a.page_location, '?')[OFFSET(0)] AS page_location,
    a.is_entrance,
    DENSE_RANK() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) AS hit_number,
    ROW_NUMBER() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp) AS row_nbr
  FROM
    get_all_events_in_each_session_staging a
),
--get the hit number associated with the last page view in each session
hit_nbr_of_last_page_view_in_each_session AS (
  SELECT
    visit_identifier,
    COUNT(1) AS nbr_page_view_events,
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
    b.nbr_page_view_events,
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
    a.visit_identifier,
    b.max_hit_number,
    b.nbr_page_view_events,
    is_exit,
    a.event_name
),
first_and_last_interaction AS (
  SELECT
    visit_identifier,
    MAX(CASE WHEN engaged_session_event = 1 THEN 1 ELSE 0 END) AS session_had_an_engaged_event,
    MIN(CASE WHEN engaged_session_event = 1 THEN hit_number ELSE NULL END) AS first_interaction,
    MAX(CASE WHEN engaged_session_event = 1 THEN hit_number ELSE NULL END) AS last_interaction
  FROM
    get_all_events_in_each_session
  GROUP BY
    visit_identifier
),
final_staging AS (
  SELECT
    a.date,
    a.visit_identifier,
    a.full_visitor_id,
    a.visit_start_time,
    REGEXP_REPLACE(b.page_location, '^https://www.mozilla.org', '') AS page_path,
    SPLIT(REGEXP_REPLACE(b.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(1)
    ] AS page_path_level1,
    CASE
      WHEN b.event_name = 'page_view'
        THEN 'PAGE'
      ELSE 'EVENT'
    END AS hit_type,
    CAST(COALESCE(c.is_exit, 0) AS bool) AS is_exit,
    CAST(b.is_entrance AS bool) AS is_entrance,
    b.hit_number,
    b.event_timestamp AS hit_timestamp,
    CAST(NULL AS string) AS event_category, --GA4 has no notion of event_label, unlike GA3 (UA360)
    b.event_name,
    CAST(NULL AS string) AS event_label, --GA4 has no notion of event_label, unlike GA3 (UA360)
    CAST(NULL AS string) AS event_action, --GA4 has no notion of event_action, unlike GA3 (UA360)
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
    d.session_had_an_engaged_event AS visits, --this is the equivalent logic to totals.visits in UA
    CASE
      WHEN c.nbr_page_view_events = 1
        THEN 1
      ELSE 0
    END AS bounces, --this is the equivalent logic to totals.bounces in UA
    SAFE_DIVIDE(engagement_time_msec, 1000) AS hit_time,
    d.first_interaction,
    CAST(d.last_interaction AS float64) AS last_interaction,
    b.is_entrance AS entrances,
    COALESCE(c.is_exit, 0) AS exits,
    CAST(
      NULL AS string
    ) AS event_id, --old table defined this from event category, action, and label, which no longer exist in GA4
    SPLIT(REGEXP_REPLACE(b.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(1)
    ] AS page_level_1,
    SPLIT(REGEXP_REPLACE(b.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(2)
    ] AS page_level_2,
    SPLIT(REGEXP_REPLACE(b.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(3)
    ] AS page_level_3,
    SPLIT(REGEXP_REPLACE(b.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(4)
    ] AS page_level_4,
    SPLIT(REGEXP_REPLACE(b.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(5)
    ] AS page_level_5
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
  LEFT OUTER JOIN
    first_and_last_interaction d
    ON a.visit_identifier = d.visit_identifier
)
SELECT
  a.date,
  a.visit_identifier,
  a.full_visitor_id,
  a.visit_start_time,
  a.page_path,
  a.page_path_level1,
  a.hit_type,
  a.is_exit,
  a.is_entrance,
  a.hit_number,
  a.hit_timestamp,
  a.event_category,
  a.event_name,
  a.event_label,
  a.event_action,
  a.device_category,
  a.operating_system,
  a.language,
  a.browser,
  a.browser_version,
  a.country,
  a.source,
  a.medium,
  a.campaign,
  a.ad_content,
  a.visits,
  a.bounces,
  a.hit_time,
  a.first_interaction,
  a.last_interaction,
  a.entrances,
  a.exits,
  a.event_id,
  a.page_level_1,
  a.page_level_2,
  a.page_level_3,
  a.page_level_4,
  a.page_level_5,
  IF(
    page_level_2 IS NULL,
    CONCAT('/', page_level_1, '/'),
    ARRAY_TO_STRING(['', page_level_1, page_level_2, page_level_3, page_level_4, page_level_5], '/')
  ) AS page_name,
FROM
  final_staging a
