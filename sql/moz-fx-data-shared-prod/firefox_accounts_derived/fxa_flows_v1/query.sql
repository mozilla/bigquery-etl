WITH base AS (
  SELECT
    flow_id,
    event_type,
    `timestamp`,
    entrypoint,
    entrypoint_experiment,
    entrypoint_variation,
    utm_source,
    utm_medium,
    utm_term,
    utm_campaign,
    utm_content,
    country,
    ua_browser,
    ua_version,
    os_name,
  FROM
    `firefox_accounts.fxa_all_events`
  WHERE
    DATE(`timestamp`) >= DATE("2022-09-01")
    AND fxa_log IN ('content', 'auth', 'oauth')
    AND event_type NOT IN (
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
flow_attributes AS (
    -- @loines: any other attributes we may want to keep?
  SELECT
    flow_id,
    ARRAY_AGG(
      IF(
        `timestamp` IS NOT NULL
        AND (
          country IS NOT NULL
          OR os_name IS NOT NULL
          OR ua_browser IS NOT NULL
          OR ua_version IS NOT NULL
        ),
        STRUCT(
          `timestamp` AS flow_start_timestamp,
          country AS flow_country,
          os_name AS flow_os_name,
          ua_browser AS flow_ua_browser,
          ua_version AS flow_ua_version
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        `timestamp`
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS flow_info,
  FROM
    base
  WHERE
    flow_id IS NOT NULL
  GROUP BY
    flow_id
),
flow_entrypoints AS (
  SELECT
    flow_id,
    ARRAY_AGG(
      IF(
        entrypoint IS NOT NULL
        OR entrypoint_experiment IS NOT NULL
        OR entrypoint_variation IS NOT NULL,
        STRUCT(entrypoint, entrypoint_experiment, entrypoint_variation),
        NULL
      ) IGNORE NULLS
      ORDER BY
        `timestamp`
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS entrypoint_info
  FROM
    base
  WHERE
    flow_id IS NOT NULL
  GROUP BY
    flow_id
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
    base
  WHERE
    flow_id IS NOT NULL
  GROUP BY
    flow_id
),
flow_events AS (
  SELECT
    flow_id,
    ARRAY_AGG(
      STRUCT(
        `timestamp` AS event_timestamp,
        SPLIT(event_type, ' - ')[OFFSET(0)] AS event_category,
        SPLIT(event_type, ' - ')[OFFSET(1)] AS event_name
      )
      ORDER BY
        `timestamp`
    ) AS flow_events, -- maybe this event_type split should happen earlier in the pipeline?
  FROM
    base
  WHERE
    flow_id IS NOT NULL
  GROUP BY
    flow_id
)
SELECT
  flow_info.*,
  utm_info.*,
  flow_events.*,
FROM
  flow_attributes
LEFT JOIN
  flow_utms
USING
  (flow_id)
LEFT JOIN
  flow_entrypoints
USING
  (flow_id)
LEFT JOIN
  flow_events
USING
  (flow_id)
