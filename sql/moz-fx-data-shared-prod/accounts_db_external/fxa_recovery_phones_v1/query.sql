SELECT
  TO_HEX(uid) AS uid,
  phoneNumber,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(lastConfirmed AS INT)) AS lastConfirmed,
  SAFE.PARSE_JSON(lookupData) AS lookupData
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         uid,
         phoneNumber,
         createdAt,
         lastConfirmed,
         lookupData
       FROM
         fxa.recoveryPhones
    """
  )
