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
    AND flow_id IS NOT NULL
  GROUP BY
    flow_id
  HAVING
    LOGICAL_OR(
      service = "guardian-vpn"
      OR entrypoint LIKE "www.mozilla.org-vpn-%"
      OR utm_source LIKE "www.mozilla.org-vpn-%"
      OR (
        event_type = "fxa_rp_button - view"
        AND (
          -- The www.mozilla.org navbar CTA button was changed to link to VPN for Firefox users
          -- on 2021-09-02, then attribution was implemented for it on 2021-09-15.
          (
            service IS NULL
            AND utm_source = "www.mozilla.org"
            AND utm_campaign = "navigation"
            AND (partition_date BETWEEN "2021-09-15" AND "2021-12-08")
          )
          -- Service attribution was implemented for VPN FxA links on www.mozilla.org on 2021-12-08,
          -- but the service ended up as "undefined_oauth" until it was fixed on 2022-01-06.
          OR (
            service = "undefined_oauth"
            AND (partition_date BETWEEN "2021-12-08" AND "2022-01-06")
          )
        )
      )
      -- Even when service attribution for VPN FxA links was fixed on 2022-01-06 there was still a
      -- problem with service attribution for FxA payment events, which was fixed on 2022-03-09.
      OR (
        event_type LIKE r"fxa\_pay\_%"
        AND service = "undefined_oauth"
        AND (partition_date BETWEEN "2021-12-08" AND "2022-03-09")
      )
    )
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
