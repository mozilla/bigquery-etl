CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.monthly_active_logical_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_logical_subscriptions_v1`
