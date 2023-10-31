SELECT
  id,
  normalizedEmail,
  email,
  TO_HEX(uid) AS uid,
  SAFE_CAST(isVerified AS BOOL) AS isVerified,
  SAFE_CAST(isPrimary AS BOOL) AS isPrimary,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(verifiedAt AS INT)) AS verifiedAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         id,
         normalizedEmail,
         email,
         uid,
         isVerified,
         isPrimary,
         verifiedAt,
         createdAt
       FROM
         emails
    """
  )
