SELECT
  TO_HEX(uid) AS uid,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  uaBrowser,
  uaBrowserVersion,
  uaOS,
  uaOSVersion,
  uaDeviceType,
  lastAccessTime,
  uaFormFactor,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(authAt AS INT)) AS authAt,
  verificationMethod,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(verifiedAt AS INT)) AS verifiedAt,
  SAFE_CAST(mustVerify AS BOOL) AS mustVerify,
  providerId,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         uid,
         createdAt,
         uaBrowser,
         uaBrowserVersion,
         uaOS,
         uaOSVersion,
         uaDeviceType,
         lastAccessTime,
         uaFormFactor,
         authAt,
         verificationMethod,
         verifiedAt,
         mustVerify,
         providerId
       FROM
         fxa.sessionTokens
    """
  )
