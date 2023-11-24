SELECT
  TO_HEX(uid) AS uid,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(verifiedAt AS INT)) AS verifiedAt,
  SAFE_CAST(enabled AS BOOL) AS enabled,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         uid,
         createdAt,
         verifiedAt,
         enabled
       FROM
         fxa.recoveryKeys
    """
  )
