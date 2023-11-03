SELECT
  TO_HEX(uid) AS uid,
  normalizedEmail,
  email,
  SAFE_CAST(emailVerified AS BOOL) AS emailVerified,
  verifierVersion,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(verifierSetAt AS INT)) AS verifierSetAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  locale,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(lockedAt AS INT)) AS lockedAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(profileChangedAt AS INT)) AS profileChangedAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(keysChangedAt AS INT)) AS keysChangedAt,
  ecosystemAnonId,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(disabledAt AS INT)) AS disabledAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(metricsOptOutAt AS INT)) AS metricsOptOutAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         uid,
         normalizedEmail,
         email,
         emailVerified,
         verifierVersion,
         verifierSetAt,
         createdAt,
         locale,
         lockedAt,
         profileChangedAt,
         keysChangedAt,
         ecosystemAnonId,
         disabledAt,
         metricsOptOutAt
       FROM
         accounts
    """
  )
