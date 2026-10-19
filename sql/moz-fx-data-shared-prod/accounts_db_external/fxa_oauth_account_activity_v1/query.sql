SELECT
  TO_HEX(userId) AS userId,
  TO_HEX(clientId) AS clientId,
  scopeId,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(firstSeenAt AS INT)) AS firstSeenAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(lastSeenAt AS INT)) AS lastSeenAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-oauth-prod-prod-fxa-oauth",
    """SELECT
         userId,
         clientId,
         scopeId,
         firstSeenAt,
         lastSeenAt
       FROM
         fxa_oauth.accountActivity
    """
  )
