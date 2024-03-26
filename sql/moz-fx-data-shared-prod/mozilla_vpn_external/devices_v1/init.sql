CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.mozilla_vpn_external.devices_v1`
PARTITION BY
  DATE(updated_at)
AS
SELECT
  *
FROM
  EXTERNAL_QUERY(
    "moz-fx-guardian-prod-bfc7.us.guardian-sql-prod",
    """
    SELECT
      id,
      user_id,
      name,
      mullvad_id,
      pubkey,
      ipv4_address,
      ipv6_address,
      created_at,
      updated_at,
      uid,
      platform,
      useragent,
      unique_id
    FROM devices
    """
  )
