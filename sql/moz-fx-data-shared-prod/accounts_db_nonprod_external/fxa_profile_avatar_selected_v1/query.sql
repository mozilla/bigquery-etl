SELECT
  TO_HEX(userId) AS userId,
  TO_HEX(avatarId) AS avatarId,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa-profile",
    """SELECT
         userId,
         avatarId
       FROM
         fxa_profile.avatar_selected
    """
  )
