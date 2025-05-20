CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.monthly_active_logical_subscriptions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_logical_subscriptions_v1`
WHERE
  month_start_date < (
    SELECT
      COALESCE(MIN(month_start_date), '9999-12-31')
    FROM
      `moz-fx-data-shared-prod.subscription_platform_derived.recent_monthly_active_logical_subscriptions_v1`
  )
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.recent_monthly_active_logical_subscriptions_v1`
