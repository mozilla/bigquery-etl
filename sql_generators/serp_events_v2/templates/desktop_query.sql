WITH raw_serp_events AS (
  SELECT
    *,
    mozfun.map.get_key(event.extra, 'impression_id') AS impression_id,
    event
  FROM
    `{{ project_id }}.{{ app_name }}_stable.events_v1`,
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
    metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    ping_info.seq AS ping_seq,
    event.timestamp AS event_timestamp,
    normalized_channel,
    normalized_country_code,
    client_info.os,
    mozfun.norm.browser_version_info(client_info.app_display_version) AS browser_version_info,
    sample_id,
    ping_info.experiments,
    -- SERP impression features
    COALESCE(
      SAFE_CAST(mozfun.map.get_key(event.extra, 'is_shopping_page') AS bool),
      FALSE
    ) AS is_shopping_page,
    COALESCE(SAFE_CAST(mozfun.map.get_key(event.extra, 'is_private') AS bool), FALSE) AS is_private,
    COALESCE(
      SAFE_CAST(mozfun.map.get_key(event.extra, 'is_signed_in') AS bool),
      FALSE
    ) AS is_signed_in,
    mozfun.map.get_key(event.extra, 'provider') AS search_engine,
    mozfun.map.get_key(event.extra, 'source') AS sap_source,
    COALESCE(SAFE_CAST(mozfun.map.get_key(event.extra, 'tagged') AS bool), FALSE) AS is_tagged
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
    action,
    COUNT(*) AS num_engagements
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
    component,
    action
),
engagement_array AS (
  -- collect engagement counts for each impression into an array
  SELECT
    impression_id,
    ARRAY_AGG(STRUCT(component, action, num_engagements)) AS engagements
  FROM
    engagement_counts
  GROUP BY
    impression_id
),
ad_impression_counts AS (
  SELECT
    impression_id,
    component,
    ads_loaded,
    ads_visible,
    ads_hidden,
  FROM
    (
      SELECT
        impression_id,
        mozfun.map.get_key(event.extra, 'component') AS component,
        COALESCE(SAFE_CAST(mozfun.map.get_key(event.extra, 'ads_loaded') AS int), 0) AS ads_loaded,
        COALESCE(
          SAFE_CAST(mozfun.map.get_key(event.extra, 'ads_visible') AS int),
          0
        ) AS ads_visible,
        COALESCE(SAFE_CAST(mozfun.map.get_key(event.extra, 'ads_hidden') AS int), 0) AS ads_hidden,
        -- there should be at most 1 ad_impression event per component
        -- if there are multiple, it would be an edge case where events got duplicated
        -- enforce 1 row per impression_id/component for data cleanliness
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
ad_impression_array AS (
  -- collect component impression counts for each SERP impression into an array
  SELECT
    impression_id,
    ARRAY_AGG(
      -- change naming from 'ads' to 'elements'
      -- data is reported in 'ad_impression' events but includes non-ad page components
      STRUCT(
        component,
        ads_loaded AS num_elements_loaded,
        ads_visible AS num_elements_visible,
        -- elements explicitly hidden using JS/CSS, as by an ad blocker
        ads_hidden AS num_elements_blocked,
        -- elements that are not visible but not explicitly blocked, eg. below the fold
        ads_loaded - ads_visible - ads_hidden AS num_elements_notshowing
      )
    ) AS component_impressions
  FROM
    ad_impression_counts
  GROUP BY
    impression_id
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
  is_private,
  is_signed_in,
  search_engine,
  sap_source,
  is_tagged,
  abandon_reason,
  engagements,
  component_impressions,
  impressions.profile_group_id AS profile_group_id,
FROM
  -- 1 row per impression_id
  impressions
LEFT JOIN
  -- 1 row per impression_id with an abandonment
  abandonments
  USING (impression_id)
LEFT JOIN
  -- 1 row per impression_id with an engagement
  engagement_array
  USING (impression_id)
LEFT JOIN
  -- 1 row per impression_id with a component impression
  ad_impression_array
  USING (impression_id)
