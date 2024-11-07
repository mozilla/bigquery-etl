SELECT
  TO_HEX(id) AS id,
  TO_HEX(uid) AS uid,
  state,
  errorReasonId,
  offeringConfigId,
  `interval`,
  experiment,
  taxAddress,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(updatedAt AS INT)) AS updatedAt,
  couponCode,
  stripeCustomerId,
  email,
  amount,
  version,
  eligibilityStatus,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-prod.us.fxa-rds-prod-prod-fxa",
    """SELECT
         id,
         uid,
         state,
         errorReasonId,
         offeringConfigId,
         `interval`,
         experiment,
         taxAddress,
         createdAt,
         updatedAt,
         couponCode,
         stripeCustomerId,
         email,
         amount,
         version,
         eligibilityStatus
       FROM
         fxa.carts
    """
  )
