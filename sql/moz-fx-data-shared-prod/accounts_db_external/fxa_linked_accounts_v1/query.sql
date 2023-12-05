SELECT
  TO_HEX(uid) AS uid,
  id,
  providerId,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(authAt AS INT)) AS authAt,
  SAFE_CAST(enabled AS BOOL) AS enabled,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         uid,
         id,
         providerId,
         authAt,
         enabled
       FROM
         fxa.linkedAccounts
    """
  )
