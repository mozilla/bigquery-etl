WITH
  -- Extract all engagement events for both content types
engagement_events AS (
  SELECT
    *,
    CONCAT(user_pseudo_id, CAST(event_timestamp AS STRING)) AS event_id
  FROM
    `mozdata.sumo_ga.ga4_events`,
    UNNEST(event_params) AS ep
  WHERE
    submission_date = @submission_date
    AND event_name = 'user_engagement'
    AND ep.key = 'content_group'
    AND ep.value.string_value IN ('kb-article', 'support-forum-question-details')
),
  -- Add content_type field for easy filtering
events_with_content_type AS (
  SELECT
    ee.*,
    ep.value.string_value AS content_type
  FROM
    engagement_events ee,
    UNNEST(ee.event_params) AS ep
  WHERE
    ep.key = 'content_group'
),
  -- Filter for engaged sessions (>30 seconds)
engaged_events AS (
  SELECT
    *
  FROM
    events_with_content_type,
    UNNEST(event_params) AS ep
  WHERE
    ep.key = 'engagement_time_msec'
    AND ep.value.int_value > 1000 * 30
),
  -- Extract session IDs
events_with_sessions AS (
  SELECT
    ep.value.int_value AS ga_session_id,
    ee.*
  FROM
    engaged_events ee,
    UNNEST(ee.event_params) AS ep
  WHERE
    ep.key = 'ga_session_id'
),
  -- Map products using UDF
events_with_product AS (
  SELECT
    mozfun.sumo.map_product_slug(ep.value.string_value) AS product,
    ews.*
  FROM
    events_with_sessions ews,
    UNNEST(ews.event_params) AS ep
  WHERE
    ep.key = 'products'
)
SELECT
  DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
  product,
  CASE
    WHEN content_type = 'kb-article'
      THEN 'kb'
    WHEN content_type = 'support-forum-question-details'
      THEN 'forum_question'
  END AS content_type,
  COUNT(DISTINCT ga_session_id) AS sessions,
  COUNT(DISTINCT event_id) AS events,
  SAFE_DIVIDE(COUNT(DISTINCT event_id), COUNT(DISTINCT ga_session_id)) AS events_per_session,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  events_with_product
GROUP BY
  event_date,
  product,
  content_type
HAVING
  event_date = @submission_date
