SELECT
  id,
  emailType,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         id,
         emailType
       FROM
         fxa.emailTypes
    """
  )
