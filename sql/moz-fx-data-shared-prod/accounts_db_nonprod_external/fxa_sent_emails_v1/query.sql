SELECT
  id,
  TO_HEX(uid) AS uid,
  emailTypeId,
  params,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(sentAt AS INT)) AS sentAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         id,
         uid,
         emailTypeId,
         params,
         sentAt
       FROM
         fxa.sentEmails
    """
  )
