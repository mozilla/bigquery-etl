SELECT
  TO_HEX(uid) AS uid,
  deviceId,
  commandId,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         uid,
         deviceId,
         commandId
       FROM
         fxa.deviceCommands
    """
  )
