WITH fxa_attribution AS (
  SELECT
    fxa_uid,
    MIN(flow_started) AS flow_started,
    ARRAY_AGG(attribution ORDER BY flow_started LIMIT 1)[OFFSET(0)] AS attribution,
  FROM
    fxa_attribution_v1
  CROSS JOIN
    UNNEST(fxa_uids)
  WHERE
    attribution IS NOT NULL
  GROUP BY
    fxa_uid
)
SELECT
  id,
  fxa_uid,
  created_at,
  fxa_attribution.attribution,
FROM
  mozilla_vpn_external.users_v1
LEFT JOIN
  fxa_attribution
ON
  users_v1.fxa_uid = fxa_attribution.fxa_uid
  -- only use fxa attribution from login flows that started before user creation
  AND fxa_attribution.flow_started < users_v1.created_at
