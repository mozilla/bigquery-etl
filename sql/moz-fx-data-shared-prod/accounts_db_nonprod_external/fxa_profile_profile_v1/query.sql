SELECT
  TO_HEX(userId) AS userId,
  displayName,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa-profile",
    """SELECT
         userId,
         displayName
       FROM
         fxa_profile.profile
    """
  )
