SELECT
  commandId,
  commandName,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         commandId,
         commandName
       FROM
         fxa.deviceCommandIdentifiers
    """
  )