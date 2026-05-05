SELECT
  id,
  TO_HEX(uid) AS uid,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         id,
         uid
       FROM
         fxa.recoveryCodes
    """
  )
