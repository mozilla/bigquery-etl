SELECT
  TO_HEX(uid) AS uid,
  TO_HEX(credentialId) AS credentialId,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         uid,
         credentialId,
         createdAt
       FROM
         fxa.passkeyWraps
    """
  )
