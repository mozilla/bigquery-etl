-- Query for ga_derived.www_site_hits_v2
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
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
get_all_events_in_each_session AS (
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
    a.event_name AS event_label,
    a.event_timestamp,
    device.category AS category,
    device.operating_system AS operating_system,
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
)

SELECT
a.date,
a.visit_identifier,
a.full_visitor_id,
a.visit_start_time,
/*
? AS page_path,
? AS page_path_level1,
? AS hit_type,
? AS is_exit,
*/
b.is_entrance,
/*
? AS hit_number,
? AS event_category,
*/
b.event_label,
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
? AS hit_time,
? AS first_interaction,
? AS last_interaction,
? AS entrances,
? AS exits,
? AS event_id,
? AS page_level_1,
? AS page_level_2,
? AS page_level_3,
? AS page_level_4,
? AS page_level_5,
? AS page_name
*/
FROM get_session_start_time a
LEFT OUTER JOIN
get_all_events_in_each_session b
ON a.date = b.date
AND a.visit_identifier = b.visit_identifier
AND a.full_visitor_id = b.full_visitor_id
