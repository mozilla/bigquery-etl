SELECT
  CAST(flow_id AS INTEGER) AS user_id,
  attribution
FROM
  `moz-fx-data-shared-prod.mozilla_vpn_derived.fxa_attribution_v1`
WHERE
  flow_started IS NULL
