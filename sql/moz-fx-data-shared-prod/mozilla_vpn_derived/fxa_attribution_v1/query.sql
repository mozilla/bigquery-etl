WITH fxa_content_auth_stdout_events AS (
  SELECT
    DATE(`timestamp`) AS partition_date,
    `timestamp`,
    service,
    oauth_client_id,
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
    `moz-fx-data-shared-prod.firefox_accounts.fxa_all_events`
  WHERE
    fxa_log IN ('content', 'auth', 'stdout', 'payments')
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
    AND flow_id IS NOT NULL
    AND (
      -- Service attribution was implemented for VPN FxA links on 2021-12-08, so past that date we
      -- include all events here and use the HAVING clause to filter down to flows involving VPN.
      partition_date > "2021-12-08"
      -- Prior to that we use the attribution events filter that was in place at the time.
      OR service = "guardian-vpn"
      OR (
        (service IS NULL OR service = "undefined_oauth")
        AND (event_type = "fxa_rp_button - view" OR event_type LIKE r"fxa\_pay\_%")
      )
    )
  GROUP BY
    flow_id
  HAVING
    LOGICAL_OR(
      service = "guardian-vpn"
      -- In the past the FxA payment server didn't set the service based on the VPN OAuth client ID,
      -- and for a while Bedrock incorrectly passed "guardian-vpn" as the OAuth client ID.
      OR oauth_client_id IN ("e6eb0d1e856335fc", "guardian-vpn")
      -- Prior to service attribution for VPN FxA links being implemented on 2021-12-08 we rely on
      -- the attribution events filter in the WHERE clause that was in place at the time.
      OR partition_date <= "2021-12-08"
    )
  UNION ALL
  SELECT
    *
  FROM
    `mozilla_vpn_derived.fxa_attribution_v1`
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
