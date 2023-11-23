SELECT
  TO_HEX(uid) AS uid,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  tries,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         uid,
         createdAt,
         tries
       FROM
         fxa.passwordForgotTokens
    """
  )
