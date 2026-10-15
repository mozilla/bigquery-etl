CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stmo.object_views_daily`
AS
-- user_id is null when Redash authenticates a request with a query or dashboard API key instead
-- of a login. Nearly all of these are embedded visualizations, mostly in Confluence pages, plus a
-- few public dashboard links. They count as views but not users.
WITH view_events AS (
  SELECT
    events.created_at,
    events.object_type,
    SAFE_CAST(events.object_id AS INT64) AS object_id,
    events.user_id,
    users.email AS user_email,
    COALESCE(LAX_BOOL(events.additional_properties.dashboard), FALSE) AS from_dashboard,
    COALESCE(LAX_BOOL(events.additional_properties.embed), FALSE) AS from_embed,
    -- Only events logged by the web page have a screen resolution, not ones logged by the server
    events.additional_properties.screen_resolution IS NOT NULL AS logged_by_page,
    events.additional_properties.api_key IS NOT NULL AS used_api_key,
  FROM
    `moz-fx-data-shared-prod.stmo_external.events_v1` AS events
  LEFT JOIN
    `moz-fx-data-shared-prod.stmo_external.users_v1` AS users
    ON users.id = events.user_id
  WHERE
    events.created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY))
    AND events.action = 'view'
    AND events.object_type IN ('dashboard', 'query', 'visualization')
    -- Only browsers. Scripts using the API (python-requests, curl, node, and so on, including the
    -- DataHub ingestion), bots, and headless browsers taking screenshots are dropped.
    AND STARTS_WITH(LAX_STRING(events.additional_properties.user_agent), 'Mozilla/')
    AND NOT REGEXP_CONTAINS(
      LAX_STRING(events.additional_properties.user_agent),
      r'(?i)bot|crawl|spider|headless'
    )
    -- Accounts used by the DataHub Redash ingestion at some point,
    -- which views every published query and dashboard on each run
    AND COALESCE(users.email, '') NOT IN (
      'wichan@mozilla.com',
      'redash-datahub@mozilla.com',
      -- PollBot, a bot that polls Redash throughout the day
      'nobody@mozilla.org'
    )
  -- Drop every view by a user on a day they open 100 or more distinct dashboards, which only
  -- happens when a script crawls Redash. People rarely open more than 30 in a day. Views with
  -- no user are kept, since they aren't one person.
  QUALIFY
    events.user_id IS NULL
    OR COUNT(DISTINCT IF(events.object_type = 'dashboard', events.object_id, NULL)) OVER (
      PARTITION BY
        events.user_id,
        DATE(events.created_at)
    ) < 100
),
-- A dashboard open logs one view from the page and one from the server's API response, so only
-- the page's is counted. Public dashboard links only log the server's, which has no user.
dashboard_views AS (
  SELECT
    created_at,
    object_id AS dashboard_id,
    user_email,
  FROM
    view_events
  WHERE
    object_type = 'dashboard'
    AND (logged_by_page OR user_id IS NULL)
),
-- Dashboard widgets and embeds log visualization views. Each load also logs a query view, which
-- query_page_views leaves out.
visualization_views AS (
  SELECT
    created_at,
    object_id AS visualization_id,
    user_email,
  FROM
    view_events
  WHERE
    object_type = 'visualization'
    AND (from_dashboard OR from_embed)
),
-- The query page opens on this visualization unless the URL names another one
default_visualizations AS (
  SELECT
    query_id,
    MIN(id) AS visualization_id,
  FROM
    `moz-fx-data-shared-prod.stmo_external.visualizations_v1`
  GROUP BY
    query_id
),
-- Query views that aren't the second event of a dashboard widget or embed load
query_page_views AS (
  SELECT
    view_events.created_at,
    default_visualizations.visualization_id,
    view_events.user_email,
  FROM
    view_events
  INNER JOIN
    default_visualizations
    ON default_visualizations.query_id = view_events.object_id
  WHERE
    view_events.object_type = 'query'
    AND NOT view_events.from_dashboard
    AND NOT view_events.used_api_key
),
object_events AS (
  SELECT
    'visualization' AS object_type,
    visualization_id AS object_id,
    created_at,
    user_email,
  FROM
    visualization_views
  UNION ALL
  SELECT
    'visualization' AS object_type,
    visualization_id AS object_id,
    created_at,
    user_email,
  FROM
    query_page_views
  UNION ALL
  SELECT
    'dashboard' AS object_type,
    dashboard_id AS object_id,
    created_at,
    user_email,
  FROM
    dashboard_views
)
SELECT
  object_type,
  object_id,
  DATE(created_at) AS submission_date,
  user_email,
  COUNT(*) AS views,
  MAX(created_at) AS last_viewed_at,
FROM
  object_events
GROUP BY
  object_type,
  object_id,
  submission_date,
  user_email
