WITH fxa_content_auth_stdout_events AS (
  SELECT
    DATE(`timestamp`) AS partition_date,
    `timestamp`,
    service,
    event_type,
    flow_id,
    user_id,
    entrypoint_experiment,
    entrypoint_variation,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term,
  FROM
    mozdata.firefox_accounts.fxa_content_auth_stdout_events
),
flows AS (
  SELECT
    flow_id,
    MIN(`timestamp`) AS flow_started,
    ARRAY_AGG(DISTINCT user_id IGNORE NULLS) AS fxa_uids,
    ARRAY_AGG(
      IF(
        entrypoint_experiment IS NOT NULL
        OR entrypoint_variation IS NOT NULL
        OR utm_campaign IS NOT NULL
        OR utm_content IS NOT NULL
        OR utm_medium IS NOT NULL
        OR utm_source IS NOT NULL
        OR utm_term IS NOT NULL,
        STRUCT(
          `timestamp`,
          entrypoint_experiment,
          entrypoint_variation,
          utm_campaign,
          utm_content,
          utm_medium,
          utm_source,
          utm_term
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        `timestamp`
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS attribution,
  FROM
    fxa_content_auth_stdout_events
  WHERE
    IF(@date IS NULL, partition_date < CURRENT_DATE, partition_date = @date)
    -- cannot filter service because
    AND (
      service = "guardian-vpn"
      -- service is missing for these event types
      OR (
        (service IS NULL OR service = "undefined_oauth")
        AND (event_type = "fxa_rp_button - view" OR event_type LIKE "fxa_pay_%")
      )
    )
    AND flow_id IS NOT NULL
  GROUP BY
    flow_id
  UNION ALL
  SELECT
    *
  FROM
    fxa_attribution_v1
)
SELECT
  flow_id,
  MIN(flow_started) AS flow_started,
  ARRAY_AGG(DISTINCT fxa_uid IGNORE NULLS) AS fxa_uids,
  -- preserve earliest non-null attribution
  ARRAY_AGG(attribution IGNORE NULLS ORDER BY attribution.timestamp LIMIT 1)[
    SAFE_OFFSET(0)
  ] AS attribution,
FROM
  flows
LEFT JOIN
  UNNEST(fxa_uids) AS fxa_uid
GROUP BY
  flow_id
