SELECT
  name,
  value,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         name,
         value
       FROM
         fxa.dbMetadata
    """
  )
