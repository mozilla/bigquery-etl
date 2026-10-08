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
  FROM
    `moz-fx-data-shared-prod.stmo_external.events_v1` AS events
  LEFT JOIN
    `moz-fx-data-shared-prod.stmo_external.users_v1` AS users
    ON users.id = events.user_id
  WHERE
    events.created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY))
    AND events.action = 'view'
    AND events.object_type IN ('dashboard', 'query', 'visualization')
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
-- Redash usually logs a dashboard open as two view events less than a second apart, one from the
-- page and one from its API request, so a view within 5 seconds of the same user's previous view
-- of the dashboard is dropped. Views with no user can't be told apart, so they're all kept.
dashboard_views AS (
  SELECT
    created_at,
    object_id AS dashboard_id,
    user_email,
  FROM
    view_events
  WHERE
    object_type = 'dashboard'
  QUALIFY
    user_id IS NULL
    OR COALESCE(
      TIMESTAMP_DIFF(
        created_at,
        LAG(created_at) OVER (PARTITION BY user_id, object_id ORDER BY created_at),
        MILLISECOND
      ) >= 5000,
      TRUE
    )
),
-- Dashboard widgets and embeds log visualization views. Each load also logs a query view, which
-- query_page_views drops.
visualization_views AS (
  SELECT
    view_events.created_at,
    view_events.object_id AS visualization_id,
    visualizations.query_id,
    view_events.user_id,
    view_events.user_email,
  FROM
    view_events
  INNER JOIN
    `moz-fx-data-shared-prod.stmo_external.visualizations_v1` AS visualizations
    ON visualizations.id = view_events.object_id
  WHERE
    view_events.object_type = 'visualization'
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
  LEFT JOIN
    visualization_views
    ON visualization_views.query_id = view_events.object_id
    AND visualization_views.user_id = view_events.user_id
    AND visualization_views.created_at
    BETWEEN TIMESTAMP_SUB(view_events.created_at, INTERVAL 10 SECOND)
    AND TIMESTAMP_ADD(view_events.created_at, INTERVAL 10 SECOND)
  WHERE
    view_events.object_type = 'query'
    -- Query views with no user are the second event of an embed load. They can't be matched to
    -- their visualization view by user, so they're dropped.
    AND view_events.user_id IS NOT NULL
    AND visualization_views.visualization_id IS NULL
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
