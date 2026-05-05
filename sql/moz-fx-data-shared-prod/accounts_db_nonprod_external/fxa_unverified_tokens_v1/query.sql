SELECT
  TO_HEX(uid) AS uid,
  SAFE_CAST(mustVerify AS BOOL) AS mustVerify,
  SAFE.TIMESTAMP_MILLIS(
    SAFE_CAST(tokenVerificationCodeExpiresAt AS INT)
  ) AS tokenVerificationCodeExpiresAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         uid,
         mustVerify,
         tokenVerificationCodeExpiresAt
       FROM
         fxa.unverifiedTokens
    """
  )
