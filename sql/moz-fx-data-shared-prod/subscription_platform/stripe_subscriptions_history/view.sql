CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.stripe_subscriptions_history`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_history_v1`
