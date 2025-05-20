CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.daily_active_logical_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_logical_subscriptions_v1`
WHERE
  `date` < (
    SELECT
      COALESCE(MIN(`date`), '9999-12-31')
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.recent_daily_active_logical_subscriptions_v1`
  )
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.recent_daily_active_logical_subscriptions_v1`
