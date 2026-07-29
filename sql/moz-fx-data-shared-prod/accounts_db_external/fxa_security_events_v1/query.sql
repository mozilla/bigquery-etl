SELECT
  id,
  TO_HEX(uid) AS uid,
  nameId,
  SAFE_CAST(verified AS BOOL) AS verified,
  TO_HEX(ipAddrHmac) AS ipAddrHmac,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  TO_HEX(tokenVerificationId) AS tokenVerificationId,
  ipAddr,
  SAFE.PARSE_JSON(additionalInfo) AS additionalInfo,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         id,
         uid,
         nameId,
         verified,
         ipAddrHmac,
         createdAt,
         tokenVerificationId,
         ipAddr,
         additionalInfo
       FROM
         fxa.securityEvents
    """
  )
