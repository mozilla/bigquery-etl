WITH raw_serp_events AS (
  SELECT
    *,
    mozfun.map.get_key(event.extra, 'impression_id') AS impression_id,
    event
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`,
    UNNEST(events) AS event
  WHERE
    event.category = 'serp'
    -- allow events related to an impression_id to span 2 submission dates
    -- we restrict to event sequences started on a single date below
    AND DATE(submission_timestamp) >= DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE(submission_timestamp) <= @submission_date
),
serp_event_counts AS (
  SELECT
    impression_id,
    COUNTIF(event.name = 'impression') AS n_impressions,
    COUNTIF(event.name = 'engagement') AS n_engagements,
    COUNTIF(event.name = 'abandonment') AS n_abandonments,
  FROM
    raw_serp_events
  GROUP BY
    impression_id
),
filtered_impression_ids AS (
  -- select serp sessions/impression IDs with the expected combinations of events
  SELECT
    impression_id
  FROM
    serp_event_counts
  WHERE
    n_impressions = 1
    AND ((n_engagements >= 1 AND n_abandonments = 0) OR (n_engagements = 0 AND n_abandonments = 1))
),
serp_events AS (
  SELECT
    *
  FROM
    raw_serp_events
  INNER JOIN
    filtered_impression_ids
    USING (impression_id)
),
impressions AS (
  -- pull top-level fields from the impression event
  SELECT
    impression_id,
    submission_timestamp,
    client_info.client_id AS glean_client_id,
    metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
    ping_info.seq AS ping_seq,
    event.timestamp AS event_timestamp,
    normalized_channel,
    normalized_country_code,
    client_info.os,
    mozfun.norm.browser_version_info(client_info.app_display_version) AS browser_version_info,
    sample_id,
    ping_info.experiments,
    -- SERP session characteristics
    CAST(mozfun.map.get_key(event.extra, 'is_shopping_page') AS bool) AS is_shopping_page,
    mozfun.map.get_key(event.extra, 'provider') AS search_engine,
    mozfun.map.get_key(event.extra, 'source') AS sap_source,
    CAST(mozfun.map.get_key(event.extra, 'tagged') AS bool) AS is_tagged
  FROM
    serp_events
  WHERE
    event.name = 'impression'
    -- restrict to sessions that started on the target submission date
    AND DATE(submission_timestamp) = DATE_SUB(@submission_date, INTERVAL 1 DAY)
),
abandonments AS (
  SELECT
    impression_id,
    mozfun.map.get_key(event.extra, 'reason') AS abandon_reason
  FROM
    serp_events
  WHERE
    event.name = 'abandonment'
),
engagement_counts AS (
  SELECT
    impression_id,
    component,
    COUNTIF(action = 'clicked') AS num_clicks,
    COUNTIF(action = 'expanded') AS num_expands,
    COUNTIF(action = 'submitted') AS num_submits,
  FROM
    (
    -- 1 row per engagement event
      SELECT
        impression_id,
        mozfun.map.get_key(event.extra, 'action') AS action,
        mozfun.map.get_key(event.extra, 'target') AS component,
      FROM
        serp_events
      WHERE
        event.name = 'engagement'
    )
  GROUP BY
    impression_id,
    component
),
engaged_sessions AS (
  -- indicator for sessions with overall nonzero engagements
  SELECT DISTINCT
    impression_id,
    TRUE AS is_engaged
  FROM
    engagement_counts
  GROUP BY
    impression_id
),
ad_impression_counts AS (
  SELECT
    impression_id,
    component,
    ads_loaded AS num_ads_loaded_reported,
    ads_visible AS num_ads_visible_reported,
    ads_hidden AS num_ads_hidden_reported,
  FROM
    (
      SELECT
        impression_id,
        mozfun.map.get_key(event.extra, 'component') AS component,
        CAST(mozfun.map.get_key(event.extra, 'ads_loaded') AS int) AS ads_loaded,
        CAST(mozfun.map.get_key(event.extra, 'ads_visible') AS int) AS ads_visible,
        CAST(mozfun.map.get_key(event.extra, 'ads_hidden') AS int) AS ads_hidden,
      -- there should be at most 1 ad_impression event per component
      -- if there are multiple, it would be an edge case where events got duplicated
      -- enforce 1 row per session/component for data cleanliness
        RANK() OVER (
          PARTITION BY
            impression_id,
            mozfun.map.get_key(event.extra, 'component')
          ORDER BY
            event.timestamp
        ) AS i
      FROM
        serp_events
      WHERE
        event.name = 'ad_impression'
    )
  WHERE
    i = 1
),
ad_sessions AS (
  -- indicator for sessions with overall nonzero ad impressions
  SELECT DISTINCT
    impression_id,
    TRUE AS has_ads_loaded
  FROM
    ad_impression_counts
  GROUP BY
    impression_id
),
component_counts AS (
  -- join engagements and ad impressions into a single table
  -- 1 row for each session/component that had either an impression or an engagement
  SELECT
    impression_id,
    component,
    COALESCE(num_clicks, 0) AS num_clicks,
    COALESCE(num_expands, 0) AS num_expands,
    COALESCE(num_submits, 0) AS num_submits,
    COALESCE(num_ads_loaded_reported, 0) AS num_ads_loaded_reported,
    COALESCE(num_ads_visible_reported, 0) AS num_ads_visible_reported,
    COALESCE(num_ads_hidden_reported, 0) AS num_ads_hidden_reported,
    -- ad blocker usage is inferred when all ads are hidden
    COALESCE(
      num_ads_loaded_reported > 0
      AND num_ads_hidden_reported = num_ads_loaded_reported,
      FALSE
    ) AS ad_blocker_inferred,
  FROM
    engagement_counts
  FULL JOIN
    ad_impression_counts
    USING (impression_id, component)
)
SELECT
  impression_id,
  DATE(submission_timestamp) AS submission_date,
  glean_client_id,
  legacy_telemetry_client_id,
  ping_seq,
  event_timestamp,
  normalized_channel,
  normalized_country_code,
  os,
  browser_version_info,
  sample_id,
  experiments,
  is_shopping_page,
  search_engine,
  sap_source,
  is_tagged,
  COALESCE(is_engaged, FALSE) AS is_engaged,
  COALESCE(has_ads_loaded, FALSE) AS has_ads_loaded,
  abandon_reason,
  component,
  -- indicator for components that can have ad impressions
  -- engagements are recorded for these and other components
  component IN (
    'ad_carousel',
    'ad_image_row',
    'ad_link',
    'ad_sidebar',
    'ad_sitelink',
    'refined_search_buttons',
    'shopping_tab'
  ) AS is_ad_component,
  COALESCE(num_clicks, 0) AS num_clicks,
  COALESCE(num_expands, 0) AS num_expands,
  COALESCE(num_submits, 0) AS num_submits,
  COALESCE(num_ads_loaded_reported, 0) AS num_ads_loaded_reported,
  COALESCE(num_ads_visible_reported, 0) AS num_ads_visible_reported,
  COALESCE(num_ads_hidden_reported, 0) AS num_ads_hidden_reported,
  COALESCE(ad_blocker_inferred, FALSE) AS ad_blocker_inferred,
  COALESCE(
    CASE
    -- when an ad blocker is active, this should be 0
      WHEN ad_blocker_inferred
        THEN num_ads_visible_reported
      ELSE
    -- when no ad blocker is active, count ads reported as 'hidden' as visible
    -- this is an edge case where the hidden count may not be reliable
        num_ads_visible_reported + num_ads_hidden_reported
    END,
    0
  ) AS num_ads_showing,
  -- ads that are not visible but not blocked by an ad blocker are considered 'not showing'
  COALESCE(
    num_ads_loaded_reported - num_ads_visible_reported - num_ads_hidden_reported,
    0
  ) AS num_ads_notshowing,
FROM
  -- 1 row per impression_id
  impressions
LEFT JOIN
  -- 1 row per impression_id with nonzero engagements
  engaged_sessions
  USING (impression_id)
LEFT JOIN
  -- 1 row per impression_id with nonzero ad impressions
  ad_sessions
  USING (impression_id)
LEFT JOIN
  -- 1 row per impression_id with an abandonment
  abandonments
  USING (impression_id)
LEFT JOIN
  -- expands to 1 row per impression_id per component that had either an engagement or an ad impression
  component_counts
  USING (impression_id)
