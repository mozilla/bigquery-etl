SELECT
  TO_HEX(uid) AS uid,
  scope,
  service,
  TO_HEX(clientId) AS clientId,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(firstAuthorizedTosAt AS INT)) AS firstAuthorizedTosAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(lastAuthorizedTosAt AS INT)) AS lastAuthorizedTosAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-oauth-prod-prod-fxa-oauth",
    """SELECT
         uid,
         scope,
         service,
         clientId,
         firstAuthorizedTosAt,
         lastAuthorizedTosAt
       FROM
         fxa_oauth.accountAuthorizations
    """
  )
