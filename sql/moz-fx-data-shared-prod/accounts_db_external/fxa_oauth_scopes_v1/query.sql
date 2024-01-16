SELECT
  scope,
  SAFE_CAST(hasScopedKeys AS BOOL) AS hasScopedKeys,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-oauth-prod-prod-fxa-oauth",
    """SELECT
         scope,
         hasScopedKeys
       FROM
         fxa_oauth.scopes
    """
  )
