SELECT
  TO_HEX(id) AS id,
  name,
  imageUri,
  redirectUri,
  SAFE_CAST(canGrant AS BOOL) AS canGrant,
  SAFE_CAST(publicClient AS BOOL) AS publicClient,
  createdAt,
  SAFE_CAST(trusted AS BOOL) AS trusted,
  allowedScopes,
  notes,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-oauth-prod-prod-fxa-oauth",
    """SELECT
         id,
         name,
         imageUri,
         redirectUri,
         canGrant,
         publicClient,
         createdAt,
         trusted,
         allowedScopes,
         notes
       FROM
         fxa_oauth.clients
    """
  )
