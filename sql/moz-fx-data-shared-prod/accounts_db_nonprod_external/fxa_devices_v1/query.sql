SELECT
  TO_HEX(uid) AS uid,
  TO_HEX(id) AS id,
  name,
  nameUtf8,
  type,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  callbackPublicKey,
  SAFE_CAST(callbackIsExpired AS BOOL) AS callbackIsExpired,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         uid,
         id,
         name,
         nameUtf8,
         type,
         createdAt,
         callbackPublicKey,
         callbackIsExpired
       FROM
         fxa.devices
    """
  )
