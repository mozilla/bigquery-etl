SELECT
  TO_HEX(clientId) AS clientId,
  TO_HEX(userId) AS userId,
  scope,
  createdAt,
  lastUsedAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(profileChangedAt AS INT)) AS profileChangedAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-oauth-prod-prod-fxa-oauth",
    """SELECT
         clientId,
         userId,
         scope,
         createdAt,
         lastUsedAt,
         profileChangedAt
       FROM
         fxa_oauth.refreshTokens
    """
  )
