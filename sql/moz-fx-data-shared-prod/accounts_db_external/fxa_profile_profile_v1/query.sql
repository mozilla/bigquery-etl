SELECT
  TO_HEX(userId) AS userId,
  displayName,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa-profile",
    """SELECT
         userId,
         displayName
       FROM
         fxa_profile.profile
    """
  )
