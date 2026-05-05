SELECT
  TO_HEX(uid) AS uid,
  billingAgreementId,
  status,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(endedAt AS INT)) AS endedAt,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         uid,
         billingAgreementId,
         status,
         createdAt,
         endedAt
       FROM
         fxa.paypalCustomers
    """
  )
