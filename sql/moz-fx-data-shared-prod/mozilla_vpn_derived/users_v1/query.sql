WITH fxa_attribution AS (
  SELECT
    fxa_uid,
    MIN(flow_started) AS flow_started,
    ARRAY_AGG(attribution ORDER BY flow_started LIMIT 1)[OFFSET(0)] AS attribution,
  FROM
    fxa_attribution_v1
  CROSS JOIN
    UNNEST(fxa_uids) AS fxa_uid
  WHERE
    attribution IS NOT NULL
  GROUP BY
    fxa_uid
)
SELECT
  users_v1.id,
  users_v1.fxa_uid,
  users_v1.created_at,
  fxa_attribution.attribution,
FROM
  mozilla_vpn_external.users_v1
LEFT JOIN
  fxa_attribution
ON
  users_v1.fxa_uid = fxa_attribution.fxa_uid
