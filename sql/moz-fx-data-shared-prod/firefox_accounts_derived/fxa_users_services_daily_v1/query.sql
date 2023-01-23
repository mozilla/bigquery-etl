-- Should this be a persisted UDF? tier1_country
-- definition probably does not change between
-- specific queries.
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

  --
  -- This UDF is also only applicable in the context of this query.
CREATE TEMP FUNCTION udf_contains_registration(
  x ANY TYPE
) AS ( --
  EXISTS(
    SELECT
      event_type
    FROM
      UNNEST(x) AS event_type
    WHERE
      event_type IN ( --
        'fxa_reg - complete'
      )
  )
);

WITH fxa_events AS (
  SELECT
    `timestamp`,
    user_id,
    -- cert_signed is specific to sync, but these events do not have the
    -- 'service' field populated, so we fill in the service name for this special case.
    IF(
      `service` IS NULL
      AND event_type = 'fxa_activity - cert_signed',
      'sync',
      `service`
    ) AS `service`,
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
    `moz-fx-data-shared-prod.firefox_accounts.fxa_all_events`
  WHERE
    DATE(`timestamp`)
    -- 2 day time window used to make sure we can get user session attribution information
    -- which will not always be available in the same partition as active user activity
    -- ('fxa_login - complete', 'fxa_reg - complete').
    -- this includes fields such as entrypoint, utm's etc.
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
    AND event_category IN ('content', 'auth', 'oauth')
    AND event_type NOT IN ( --
      'fxa_email - bounced',
      'fxa_email - click',
      'fxa_email - sent',
      'fxa_reg - password_blocked',
      'fxa_reg - password_common',
      'fxa_reg - password_enrolled',
      'fxa_reg - password_missing',
      'fxa_sms - sent',
      'mktg - email_click',
      'mktg - email_open',
      'mktg - email_sent',
      'sync - repair_success',
      'sync - repair_triggered'
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
  -- getting the first entrypoint only the value
  -- for a flow_id
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY flow_id ORDER BY `timestamp` ASC) = 1
),
utms AS (
  SELECT DISTINCT
    flow_id,
    FIRST_VALUE(utm_term IGNORE NULLS) OVER (_window) AS utm_term,
    FIRST_VALUE(utm_medium IGNORE NULLS) OVER (_window) AS utm_medium,
    FIRST_VALUE(utm_source IGNORE NULLS) OVER (_window) AS utm_source,
    FIRST_VALUE(utm_campaign IGNORE NULLS) OVER (_window) AS utm_campaign,
    FIRST_VALUE(utm_content IGNORE NULLS) OVER (_window) AS utm_content,
  FROM
    fxa_events
  WHERE
    flow_id IS NOT NULL
  -- Need to do this dedup, as otherwise we
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY flow_id ORDER BY `timestamp` ASC) = 1
  WINDOW
    _window AS (
      PARTITION BY
        flow_id
        -- , DATE(`timestamp`)
      ORDER BY
        `timestamp` ASC
      ROWS
        UNBOUNDED PRECEDING
    )
),
windowed AS (
  SELECT
    `timestamp`,
    user_id,
    `service`,
    -- TODO: should we replace the array agg with FIRST_VALUE()
    -- over window with WINDOW OVER ROWS BETWEEM UNBOUNDED PRECEDING AND CURRENT ROW?
    -- seems we save around 8 seconds in execution time and around 10mins in slot time consumed
    udf.mode_last(ARRAY_AGG(flow_id) OVER w1_reversed) AS flow_id,
    udf.mode_last(ARRAY_AGG(country) OVER w1) AS country,
    udf.mode_last(ARRAY_AGG(`language`) OVER w1) AS `language`,
    udf.mode_last(ARRAY_AGG(app_version) OVER w1) AS app_version,
    udf.mode_last(ARRAY_AGG(os_name) OVER w1) AS os_name,
    udf.mode_last(ARRAY_AGG(os_version) OVER w1) AS os_version,
    udf.mode_last(ARRAY_AGG(ua_version) OVER w1) AS ua_version,
    udf.mode_last(ARRAY_AGG(ua_browser) OVER w1) AS ua_browser,
    udf_contains_tier1_country(ARRAY_AGG(country) OVER w1) AS seen_in_tier1_country,
    udf_contains_registration(ARRAY_AGG(event_type) OVER w1) AS registered,
    ARRAY_AGG(event_type) OVER w1_reversed AS service_events,
  FROM
    fxa_events
  WHERE
    DATE(`timestamp`) = @submission_date
    AND user_id IS NOT NULL
    AND `service` IS NOT NULL
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id, service, DATE(`timestamp`) ORDER BY `timestamp`) = 1
  WINDOW
    w1 AS (
      PARTITION BY
        user_id,
        `service`,
        DATE(`timestamp`)
      ORDER BY
        `timestamp`
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ),
    w1_reversed AS (
      PARTITION BY
        user_id,
        `service`,
        DATE(`timestamp`)
      ORDER BY
        `timestamp`
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    )
)
SELECT
  DATE(windowed.`timestamp`) AS submission_date,
  windowed.`timestamp`,
  windowed.user_id,
  windowed.`service`,
  windowed.flow_id,
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
  -- flow entrypoints
  entrypoints.entrypoint,
  -- flow utms
  utms.utm_term,
  utms.utm_medium,
  utms.utm_source,
  utms.utm_campaign,
  utms.utm_content,
FROM
  windowed
LEFT JOIN
  entrypoints
USING
  (flow_id)
LEFT JOIN
  utms
USING
  (flow_id)
WHERE
  user_id IS NOT NULL
  AND `service` IS NOT NULL
