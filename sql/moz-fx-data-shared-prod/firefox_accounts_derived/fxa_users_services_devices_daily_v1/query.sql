WITH fxa_events AS (
  SELECT
    `timestamp`,
    user_id,
    IF(service IS NULL AND event_type = 'fxa_activity - cert_signed', 'sync', service) AS service,
    device_id,
    os_name,
    flow_id,
    event_type,
    country,
    `language`,
    entrypoint,
    utm_term,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    ua_version,
    ua_browser,
  FROM
    `firefox_accounts.fxa_all_events`
  WHERE
    DATE(`timestamp`)
    -- 2 day time window used to make sure we can get user session attribution information
    -- which will not always be available in the same partition as active user activity
    -- ('fxa_login - complete', 'fxa_reg - complete').
    -- this includes fields such as entrypoint, utm's etc.
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
entrypoints AS (
  SELECT DISTINCT
    flow_id,
    entrypoint
  FROM
    fxa_events
  WHERE
    -- if both values are not set then the record
    -- cannot be used for mapping
    flow_id IS NOT NULL
    AND entrypoint IS NOT NULL
  -- in case we find multiple entrypoints for a single flow_id
  -- we only keep the first one
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY flow_id ORDER BY `timestamp` ASC) = 1
),
utms AS (
  SELECT DISTINCT
    flow_id,
    FIRST_VALUE(utm_term) OVER (_window) AS utm_term,
    FIRST_VALUE(utm_medium) OVER (_window) AS utm_medium,
    FIRST_VALUE(utm_source) OVER (_window) AS utm_source,
    FIRST_VALUE(utm_campaign) OVER (_window) AS utm_campaign,
    FIRST_VALUE(utm_content) OVER (_window) AS utm_content,
  FROM
    fxa_events
  WHERE
    flow_id IS NOT NULL
  WINDOW
    _window AS (
      PARTITION BY
        flow_id
      ORDER BY
        `timestamp` ASC
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
),
device_service_users_entries AS (
  SELECT DISTINCT
    `timestamp`,
    user_id,
    service,
    device_id,
    os_name,
    flow_id,
    event_type,
    country,
    `language`,
    ua_version,
    ua_browser,
  FROM
    fxa_events
  WHERE
    -- we only want to identify active users for the current partition.
    DATE(`timestamp`) = @submission_date
    -- Filtering out for these specific events to be consistent with the logic used by
    -- fxa_users_daily_v1 and fxa_users_services_daily_v1
    AND ((event_type IN ('fxa_login - complete', 'fxa_reg - complete') AND service IS NOT NULL))
)
SELECT
  -- device_service_users_entries
  device_service_users_entries.`timestamp`,
  device_service_users_entries.flow_id,
  device_service_users_entries.event_type,
  device_service_users_entries.user_id,
  device_service_users_entries.service,
  device_service_users_entries.device_id,
  device_service_users_entries.os_name,
  device_service_users_entries.country,
  device_service_users_entries.`language`,
  device_service_users_entries.ua_version,
  device_service_users_entries.ua_browser,
  -- entrypoints
  entrypoints.entrypoint,
  -- utms
  utms.utm_term,
  utms.utm_medium,
  utms.utm_source,
  utms.utm_campaign,
  utms.utm_content,
FROM
  device_service_users_entries
LEFT JOIN
  entrypoints
USING
  (flow_id)
LEFT JOIN
  utms
USING
  (flow_id)
WHERE
  -- making sure the user is registered
  user_id IS NOT NULL
  -- making sure there is a flow_id associated with this session
  -- the current logic relies on this value being set to retrieve
  -- its attributes correctly
  AND flow_id IS NOT NULL
  -- if either service or device_id is null then the record
  -- is useless for this model
  AND service IS NOT NULL
  AND device_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      user_id,
      service,
      device_id
    ORDER BY
      `timestamp` ASC
  ) = 1  -- this could be partitioned by first_flow_id to handle multiple device_ids being attached to a single flow_id
