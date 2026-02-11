-- GA4 Engagement Sessions Base Table (Daily Grain)
-- Single source of truth for SUMO GA4 engagement sessions
--
-- This table consolidates engagement metrics for:
-- - KB articles (content_group = 'kb-article')
-- - Forum questions (content_group = 'support-forum-question-details')
--
-- Replaces duplicated q3/q4 queries in SSSR scripts
--
-- Grain: Daily x Product x Content Type
WITH
  -- Extract all engagement events for both content types
engagement_events AS (
  SELECT
    *,
    ROW_NUMBER() OVER () AS event_id
  FROM
    `mozdata.sumo_ga.ga4_events`,
    UNNEST(event_params) AS ep
  WHERE
    event_name = 'user_engagement'
    AND event_date
    BETWEEN '20240701'
    AND '20260105'
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
-- Final aggregation by date, product, and content type
SELECT
  PARSE_DATE('%Y%m%d', event_date) AS date,
  product,
  -- Normalize content_type to simple labels
  CASE
    WHEN content_type = 'kb-article'
      THEN 'kb'
    WHEN content_type = 'support-forum-question-details'
      THEN 'forum_question'
    ELSE 'other'
  END AS content_type,
  -- Session metrics
  COUNT(DISTINCT ga_session_id) AS sessions,
  COUNT(DISTINCT event_id) AS events,
  SAFE_DIVIDE(COUNT(DISTINCT event_id), COUNT(DISTINCT ga_session_id)) AS events_per_session,
  -- Engagement metrics
  AVG(
    (
      SELECT
        ep.value.int_value
      FROM
        UNNEST(event_params) AS ep
      WHERE
        ep.key = 'engagement_time_msec'
    )
  ) / 1000.0 AS avg_engagement_time_seconds,
  -- ETL metadata
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  events_with_product
GROUP BY
  date,
  product,
  content_type
