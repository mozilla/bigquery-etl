SELECT
  TO_HEX(id) AS id,
  url,
  TO_HEX(userId) AS userId,
  providerId,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa-profile",
    """SELECT
         id,
         url,
         userId,
         providerId
       FROM
         fxa_profile.avatars
    """
  )
