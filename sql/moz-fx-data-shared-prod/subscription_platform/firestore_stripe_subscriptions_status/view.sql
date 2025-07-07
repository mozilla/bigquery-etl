CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.firestore_stripe_subscriptions_status`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.firestore_stripe_subscriptions_status_v1`
