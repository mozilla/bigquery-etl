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
  JOIN
    UNNEST(event_params) e
  WHERE
    e.key = 'ga_session_id'
    AND _TABLE_SUFFIX = SAFE.FORMAT_DATE('%Y%m%d', @submission_date)
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
    ROW_NUMBER() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) AS row_nbr
  FROM
    get_all_events_in_each_session_staging a
),
--get the hit number associated with the last page view in each session
row_nbr_of_last_page_view_in_each_session AS (
  SELECT
    visit_identifier,
    COUNT(1) AS nbr_page_view_events,
    MAX(row_nbr) AS max_row_number
  FROM
    get_all_events_in_each_session
  WHERE
    event_name = 'page_view'
  GROUP BY
    visit_identifier
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
    all_sessions.date,
    all_sessions.visit_identifier,
    all_sessions.full_visitor_id,
    all_sessions.visit_start_time,
    REGEXP_REPLACE(all_events.page_location, '^https://www.mozilla.org', '') AS page_path,
    SPLIT(REGEXP_REPLACE(all_events.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(1)
    ] AS page_path_level1,
    CASE
      WHEN all_events.event_name = 'page_view'
        THEN 'PAGE'
      ELSE 'EVENT'
    END AS hit_type,
    CAST(CASE WHEN exits.max_row_number = all_events.row_nbr THEN 1 ELSE 0 END AS bool) AS is_exit,
    CAST(all_events.is_entrance AS bool) AS is_entrance,
    all_events.hit_number,
    all_events.event_timestamp AS hit_timestamp,
    CAST(NULL AS string) AS event_category, --GA4 has no notion of event_label, unlike GA3 (UA360)
    all_events.event_name,
    CAST(NULL AS string) AS event_label, --GA4 has no notion of event_label, unlike GA3 (UA360)
    CAST(NULL AS string) AS event_action, --GA4 has no notion of event_action, unlike GA3 (UA360)
    all_events.device_category,
    all_events.operating_system,
    all_events.language,
    all_events.browser,
    all_events.browser_version,
    all_events.country,
    all_events.source,
    all_events.medium,
    all_events.campaign,
    all_events.ad_content,
    engmgt.session_had_an_engaged_event AS visits, --this is the equivalent logic to totals.visits in UA
    CASE
      WHEN exits.nbr_page_view_events = 1
        THEN 1
      ELSE 0
    END AS bounces, --this is the equivalent logic to totals.bounces in UA
    SAFE_DIVIDE(engagement_time_msec, 1000) AS hit_time,
    engmgt.first_interaction,
    CAST(engmgt.last_interaction AS float64) AS last_interaction,
    all_events.is_entrance AS entrances,
    COALESCE(CASE WHEN exits.max_row_number = all_events.row_nbr THEN 1 ELSE 0 END, 0) AS exits,
    CAST(
      NULL AS string
    ) AS event_id, --old table defined this from event category, action, and label, which no longer exist in GA4
    SPLIT(REGEXP_REPLACE(all_events.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(1)
    ] AS page_level_1,
    SPLIT(REGEXP_REPLACE(all_events.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(2)
    ] AS page_level_2,
    SPLIT(REGEXP_REPLACE(all_events.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(3)
    ] AS page_level_3,
    SPLIT(REGEXP_REPLACE(all_events.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(4)
    ] AS page_level_4,
    SPLIT(REGEXP_REPLACE(all_events.page_location, '^https://www.mozilla.org', ''), '/')[
      SAFE_OFFSET(5)
    ] AS page_level_5
  FROM
    get_session_start_time all_sessions
  LEFT OUTER JOIN
    get_all_events_in_each_session all_events
    ON all_sessions.date = all_events.date
    AND all_sessions.visit_identifier = all_events.visit_identifier
    AND all_sessions.full_visitor_id = all_events.full_visitor_id
  LEFT OUTER JOIN
    row_nbr_of_last_page_view_in_each_session exits
    ON all_sessions.visit_identifier = exits.visit_identifier
  LEFT OUTER JOIN
    first_and_last_interaction engmgt
    ON all_sessions.visit_identifier = engmgt.visit_identifier
)
SELECT
  final.date,
  final.visit_identifier,
  final.full_visitor_id,
  final.visit_start_time,
  final.page_path,
  final.page_path_level1,
  final.hit_type,
  final.is_exit,
  final.is_entrance,
  final.hit_number,
  final.hit_timestamp,
  final.event_category,
  final.event_name,
  final.event_label,
  final.event_action,
  final.device_category,
  final.operating_system,
  final.language,
  final.browser,
  final.browser_version,
  final.country,
  final.source,
  final.medium,
  final.campaign,
  final.ad_content,
  final.visits,
  final.bounces,
  final.hit_time,
  final.first_interaction,
  final.last_interaction,
  final.entrances,
  final.exits,
  final.event_id,
  final.page_level_1,
  final.page_level_2,
  final.page_level_3,
  final.page_level_4,
  final.page_level_5,
  IF(
    page_level_2 IS NULL,
    CONCAT('/', page_level_1, '/'),
    ARRAY_TO_STRING(['', page_level_1, page_level_2, page_level_3, page_level_4, page_level_5], '/')
  ) AS page_name,
FROM
  final_staging final
