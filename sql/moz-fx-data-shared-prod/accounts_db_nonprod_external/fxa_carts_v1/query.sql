SELECT
  TO_HEX(id) AS id,
  TO_HEX(uid) AS uid,
  state,
  errorReasonId,
  offeringConfigId,
  `interval`,
  experiment,
  taxAddress,
  currency,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(createdAt AS INT)) AS createdAt,
  SAFE.TIMESTAMP_MILLIS(SAFE_CAST(updatedAt AS INT)) AS updatedAt,
  couponCode,
  stripeCustomerId,
  stripeSubscriptionId,
  stripeIntentId,
  amount,
  version,
  eligibilityStatus,
  SAFE_CAST(isFreeTrial AS BOOL) AS isFreeTrial,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         id,
         uid,
         state,
         errorReasonId,
         offeringConfigId,
         `interval`,
         experiment,
         taxAddress,
         currency,
         createdAt,
         updatedAt,
         couponCode,
         stripeCustomerId,
         stripeSubscriptionId,
         stripeIntentId,
         amount,
         version,
         eligibilityStatus,
         isFreeTrial
       FROM
         fxa.carts
    """
  )
