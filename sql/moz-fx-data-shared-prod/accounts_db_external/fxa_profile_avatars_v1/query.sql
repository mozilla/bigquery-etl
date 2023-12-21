SELECT
  TO_HEX(id) AS id,
  url,
  TO_HEX(userId) AS userId,
  providerId,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa-profile",
    """SELECT
         id,
         url,
         userId,
         providerId
       FROM
         fxa_profile.avatars
    """
  )
