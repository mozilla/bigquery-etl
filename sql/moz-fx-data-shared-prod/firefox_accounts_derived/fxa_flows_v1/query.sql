WITH
  base AS (
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
      -- TODO: need to come up with a way to handle this 2 day window better to avoid including the same events twice
      DATE(`timestamp`)
        BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
        AND @submission_date
      AND event_category IN ('content', 'auth', 'oauth')
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
    SELECT DISTINCT
      flow_id,
      FIRST_VALUE(`timestamp` IGNORE NULLS) OVER(w1) AS flow_start_timestamp,
      FIRST_VALUE(country IGNORE NULLS) OVER(w1) AS country,
      FIRST_VALUE(os_name IGNORE NULLS) OVER(w1) AS os_name,
      FIRST_VALUE(ua_browser IGNORE NULLS) OVER(w1) AS browser_name, -- we keep this as ua_browser in fxa users tables, should we keep it the same here for consistency?
      FIRST_VALUE(ua_version IGNORE NULLS) OVER(w1) AS browser_version,
    FROM
      base
    WHERE
      flow_id IS NOT NULL
    WINDOW
      w1 AS (
          PARTITION BY
          flow_id
        ORDER BY
          `timestamp`
        ROWS BETWEEN
          UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      )
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
        `timestamp`,
        SPLIT(event_type, ' - ')[OFFSET(0)] AS category,  -- should we rename event_category in fxa_all_events? Maybe to something like fxa_log (it's a technical field anyways so that should be ok)
        SPLIT(event_type, ' - ')[OFFSET(1)] AS `event`
      ) ORDER BY `timestamp`
    ) AS flow_events, -- maybe this event_type split should happen earlier in the pipeline?
  FROM base
  WHERE flow_id IS NOT NULL
  GROUP BY flow_id
)

SELECT
  @submission_date AS submission_date,
  *
FROM
  flow_attributes
LEFT JOIN
  flow_utms USING(flow_id)
LEFT JOIN
  flow_entrypoints USING(flow_id)
LEFT JOIN
  flow_events USING(flow_id)
