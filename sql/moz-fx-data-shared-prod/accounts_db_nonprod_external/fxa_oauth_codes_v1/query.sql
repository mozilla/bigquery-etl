SELECT
  TO_HEX(clientId) AS clientId,
  TO_HEX(userId) AS userId,
  scope,
  createdAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(authAt AS INT)) AS authAt,
  amr,
  aal,
  SAFE_CAST(offline AS BOOL) AS offline,
  codeChallengeMethod,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(profileChangedAt AS INT)) AS profileChangedAt,
  TO_HEX(sessionTokenId) AS sessionTokenId,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-oauth-nonprod-stage-fxa-oauth",
    """SELECT
         clientId,
         userId,
         scope,
         createdAt,
         authAt,
         amr,
         aal,
         offline,
         codeChallengeMethod,
         profileChangedAt,
         sessionTokenId
       FROM
         fxa_oauth.codes
    """
  )
