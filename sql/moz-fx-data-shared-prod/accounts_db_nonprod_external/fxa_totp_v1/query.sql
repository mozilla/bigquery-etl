SELECT
  id,
  TO_HEX(uid) AS uid,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  SAFE_CAST(verified AS BOOL) AS verified,
  SAFE_CAST(enabled AS BOOL) AS enabled,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         id,
         uid,
         createdAt,
         verified,
         enabled
       FROM
         fxa.totp
    """
  )
