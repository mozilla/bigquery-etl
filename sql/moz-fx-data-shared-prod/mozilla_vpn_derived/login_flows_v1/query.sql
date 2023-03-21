WITH base AS (
  SELECT
    flow_id,
    MIN(`timestamp`) AS flow_started,
    MIN(
      IF(event_type IN ("fxa_login - complete", "fxa_reg - complete"), `timestamp`, NULL)
    ) AS flow_completed,
    ARRAY_AGG(
      DISTINCT IF(
        event_type IN ("fxa_login - complete", "fxa_reg - complete"),
        user_id,
        NULL
      ) IGNORE NULLS
    ) AS fxa_uids,
    LOGICAL_OR(event_type = "fxa_email_first - view") AS viewed_email_first_page,
  FROM
    `moz-fx-data-shared-prod`.firefox_accounts.fxa_all_events
  WHERE
    IF(@date IS NULL, DATE(`timestamp`) < CURRENT_DATE, DATE(`timestamp`) = @date)
    AND fxa_log IN ('content', 'auth')
    AND service = "guardian-vpn"
  GROUP BY
    flow_id
  UNION ALL
  SELECT
    *
  FROM
    `mozilla_vpn_derived.login_flows_v1`
)
SELECT
  flow_id,
  MIN(flow_started) AS flow_started,
  MIN(flow_completed) AS flow_completed,
  ARRAY_AGG(DISTINCT fxa_uid IGNORE NULLS) AS fxa_uids,
  LOGICAL_OR(viewed_email_first_page) AS viewed_email_first_page,
FROM
  base
LEFT JOIN
  UNNEST(fxa_uids) AS fxa_uid
GROUP BY
  flow_id
