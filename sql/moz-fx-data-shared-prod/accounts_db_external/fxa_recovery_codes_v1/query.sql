SELECT
  id,
  TO_HEX(uid) AS uid,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         id,
         uid
       FROM
         fxa.recoveryCodes
    """
  )
