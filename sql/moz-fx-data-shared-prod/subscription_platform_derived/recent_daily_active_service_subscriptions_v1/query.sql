SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_service_subscriptions_v1_live`
WHERE
  (
    `date` >= CURRENT_DATE() - 7
    -- Include more than the previous 7 days if necessary to avoid data gaps between this table and
    -- `daily_active_service_subscriptions_v1` (e.g. ETLs failed for multiple days and are catching up).
    OR `date` >= (
      SELECT
        MAX(`date`) + 1
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_service_subscriptions_v1`
    )
  )
  AND `date` <= CURRENT_DATE() - 1
