CREATE TEMP FUNCTION udf_contains_tier1_country(
  x ANY TYPE
) AS ( --
  EXISTS(
    SELECT
      country
    FROM
      UNNEST(x) AS country
    WHERE
      country IN ( --
        'United States',
        'France',
        'Germany',
        'United Kingdom',
        'Canada'
      )
  )
);

WITH fxa_events AS (
  SELECT
    `timestamp`,
    user_id,
    -- cert_signed is specific to sync, but these events do not have the
    -- 'service' field populated, so we fill in the service name for this special case.
    IF(service IS NULL AND event_type = 'fxa_activity - cert_signed', 'sync', service) AS service,
    os_name,
    os_version,
    app_version,
    flow_id,
    entrypoint,
    event_type,
    country,
    `language`,
    ua_version,
    ua_browser,
    utm_term,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
  FROM
    `firefox_accounts.fxa_all_events`
  WHERE
    -- 2 day time window used to make sure we can get user session attribution information
    -- which will not always be available in the same partition as active user activity
    -- ('fxa_login - complete', 'fxa_reg - complete').
    -- this includes fields such as entrypoint, utm's etc.
    DATE(`timestamp`)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
    AND fxa_log IN ('content', 'auth', 'oauth')
    AND event_type IN (
      'fxa_activity - access_token_checked',
      'fxa_activity - access_token_created',
      'fxa_activity - cert_signed',
      -- registration and login events used when deriving the first_seen table
      'fxa_reg - complete',
      'fxa_login - complete'
    )
),
flow_entrypoints AS (
  SELECT
    flow_id,
    ARRAY_AGG(
      -- if logic here so that we have an entry for
      -- flow_id even if no entrypoint is found.
      IF(entrypoint IS NOT NULL, STRUCT(flow_id, `timestamp`, entrypoint), NULL) IGNORE NULLS
      ORDER BY
        `timestamp`
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS flow_entrypoint_info
  FROM
    fxa_events
  WHERE
    flow_id IS NOT NULL
  GROUP BY
    flow_id
),
user_service_flow_entrypoints AS (
  SELECT
    user_id,
    service,
    ARRAY_AGG(flow_entrypoint_info IGNORE NULLS ORDER BY `timestamp` LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS flow_entrypoint_info,
  FROM
    fxa_events
  JOIN
    flow_entrypoints
  USING
    (flow_id)
  GROUP BY
    user_id,
    service
),
flow_utms AS (
  SELECT
    flow_id,
    ARRAY_AGG(
      IF(
        utm_campaign IS NOT NULL
        OR utm_content IS NOT NULL
        OR utm_medium IS NOT NULL
        OR utm_source IS NOT NULL
        OR utm_term IS NOT NULL,
        STRUCT(utm_campaign, utm_content, utm_medium, utm_source, utm_term),
        NULL
      ) IGNORE NULLS
      ORDER BY
        `timestamp`
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS utm_info,
  FROM
    fxa_events
  WHERE
    flow_id IS NOT NULL
  GROUP BY
    flow_id
),
user_service_utms AS (
  SELECT
    user_id,
    service,
    ARRAY_AGG(utm_info IGNORE NULLS ORDER BY `timestamp` LIMIT 1)[SAFE_OFFSET(0)] AS utm_info,
  FROM
    fxa_events
  JOIN
    flow_utms
  USING
    (flow_id)
  GROUP BY
    user_id,
    service
),
windowed AS (
  SELECT
    `timestamp`,
    user_id,
    service,
    udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(`language`) OVER w1) AS `language`,
    udf.mode_last(ARRAY_AGG(app_version) OVER w1) AS app_version,
    udf.mode_last(ARRAY_AGG(os_name) OVER w1) AS os_name,
    udf.mode_last(ARRAY_AGG(os_version) OVER w1) AS os_version,
    udf.mode_last(ARRAY_AGG(ua_version) OVER w1) AS ua_version,
    udf.mode_last(ARRAY_AGG(ua_browser) OVER w1) AS ua_browser,
    udf_contains_tier1_country(ARRAY_AGG(country) OVER w1) AS seen_in_tier1_country,
    LOGICAL_OR(event_type = 'fxa_reg - complete') OVER w1 AS registered,
    ARRAY_AGG(event_type) OVER w1 AS service_events,
  FROM
    fxa_events
  WHERE
    DATE(`timestamp`) = @submission_date
    AND user_id IS NOT NULL
    AND service IS NOT NULL
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id, service, DATE(`timestamp`) ORDER BY `timestamp`) = 1
  WINDOW
    w1 AS (
      PARTITION BY
        user_id,
        service,
        DATE(`timestamp`)
      ORDER BY
        `timestamp`
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
)
SELECT
  DATE(@submission_date) AS submission_date,
  windowed.user_id,
  windowed.service,
  windowed.country,
  windowed.`language`,
  windowed.app_version,
  windowed.os_name,
  windowed.os_version,
  windowed.ua_version,
  windowed.ua_browser,
  windowed.seen_in_tier1_country,
  windowed.registered,
  -- needed for first_seen logic
  windowed.service_events,
  -- info about first user/service flow observed
  -- on a specific day. Needed for first_seen logic
  -- flow entrypoints
  user_service_flow_entrypoints.flow_entrypoint_info AS user_service_first_daily_flow_info,
  -- flow utms
  user_service_utms.utm_info AS user_service_utm_info,
FROM
  windowed
LEFT JOIN
  user_service_flow_entrypoints
USING
  (user_id, service)
LEFT JOIN
  user_service_utms
USING
  (user_id, service)
WHERE
  user_id IS NOT NULL
  AND service IS NOT NULL
