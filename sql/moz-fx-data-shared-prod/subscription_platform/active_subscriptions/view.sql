CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.active_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.subscription_platform_derived.active_subscriptions_v1
