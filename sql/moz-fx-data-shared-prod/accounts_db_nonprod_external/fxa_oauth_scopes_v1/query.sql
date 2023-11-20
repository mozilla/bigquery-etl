SELECT
  scope,
  SAFE_CAST(hasScopedKeys AS BOOL) AS hasScopedKeys,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-oauth-nonprod-stage-fxa-oauth",
    """SELECT
         scope,
         hasScopedKeys
       FROM
         fxa_oauth.scopes
    """
  )
