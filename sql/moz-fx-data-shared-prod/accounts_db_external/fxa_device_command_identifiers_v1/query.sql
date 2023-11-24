SELECT
  commandId,
  commandName,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         commandId,
         commandName
       FROM
         fxa.deviceCommandIdentifiers
    """
  )
