SELECT
  *
FROM
  EXTERNAL_QUERY(
    "moz-fx-vpn-prod.us.vpn-bq-connection-prod",
    """
    SELECT
      uid,
      created_at,
      limited_bandwidth
    FROM proxy_entitlements
    """
  )
