SELECT
  TO_HEX(uid) AS uid,
  TO_HEX(credentialId) AS credentialId,
  TO_HEX(publicKey) AS publicKey,
  signCount,
  SAFE.PARSE_JSON(transports) AS transports,
  TO_HEX(aaguid) AS aaguid,
  name,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(lastUsedAt AS INT)) AS lastUsedAt,
  SAFE_CAST(backupEligible AS BOOL) AS backupEligible,
  SAFE_CAST(backupState AS BOOL) AS backupState,
  SAFE_CAST(prfEnabled AS BOOL) AS prfEnabled,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         uid,
         credentialId,
         publicKey,
         signCount,
         transports,
         aaguid,
         name,
         createdAt,
         lastUsedAt,
         backupEligible,
         backupState,
         prfEnabled
       FROM
         fxa.passkeys
    """
  )
