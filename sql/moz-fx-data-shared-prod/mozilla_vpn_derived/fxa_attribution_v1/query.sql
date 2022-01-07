WITH fxa_stdout_events AS (
  SELECT
    PARSE_DATE('%y%m%d', _TABLE_SUFFIX) AS partition_date,
    `timestamp`,
    jsonPayload.fields.event_properties,
    jsonPayload.fields.event_type,
    TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id,
    jsonPayload.fields.user_properties,
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.stdout_20*`
  WHERE
    jsonPayload.type = 'amplitudeEvent'
    AND jsonPayload.fields.event_type IS NOT NULL
),
fxa_content_auth_stdout_events AS (
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
    mozdata.firefox_accounts.fxa_content_auth_events
  UNION ALL
  SELECT
    partition_date,
    `timestamp`,
    JSON_VALUE(event_properties, '$.service') AS service,
    event_type,
    JSON_VALUE(user_properties, '$.flow_id') AS flow_id,
    user_id,
    JSON_VALUE(user_properties, '$.entrypoint_experiment') AS entrypoint_experiment,
    JSON_VALUE(user_properties, '$.entrypoint_variation') AS entrypoint_variation,
    JSON_VALUE(user_properties, '$.utm_campaign') AS utm_campaign,
    JSON_VALUE(user_properties, '$.utm_content') AS utm_content,
    JSON_VALUE(user_properties, '$.utm_medium') AS utm_medium,
    JSON_VALUE(user_properties, '$.utm_source') AS utm_source,
    JSON_VALUE(user_properties, '$.utm_term') AS utm_term,
  FROM
    fxa_stdout_events
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
