CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.daily_active_logical_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_logical_subscriptions_v1`
