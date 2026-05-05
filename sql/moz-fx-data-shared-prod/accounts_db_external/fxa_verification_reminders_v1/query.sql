SELECT
  TO_HEX(uid) AS uid,
  type,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         uid,
         type,
         createdAt
       FROM
         fxa.verificationReminders
    """
  )
