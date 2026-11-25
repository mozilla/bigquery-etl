SELECT
  @submission_date AS submission_date,
  COUNT(*) AS total_users,
  COUNTIF(limited_bandwidth) AS limited_bandwidth_users,
FROM
  `moz-fx-data-shared-prod.mozilla_vpn_external.proxy_entitlements_v1`
