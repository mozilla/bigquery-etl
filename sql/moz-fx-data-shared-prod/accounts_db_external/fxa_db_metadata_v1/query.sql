SELECT
  name,
  value,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         name,
         value
       FROM
         fxa.dbMetadata
    """
  )
