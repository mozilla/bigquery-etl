SELECT
  TO_HEX(uid) AS uid,
  TO_HEX(deviceId) AS deviceId,
  commandId,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         uid,
         deviceId,
         commandId
       FROM
         fxa.deviceCommands
    """
  )
