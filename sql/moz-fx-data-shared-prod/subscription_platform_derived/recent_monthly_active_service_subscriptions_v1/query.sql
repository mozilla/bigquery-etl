SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_service_subscriptions_v1_live`
WHERE
  (
    month_start_date >= CURRENT_DATE() - 7
    -- Include more than the previous 7 days if necessary to avoid data gaps between this table and
    -- `monthly_active_service_subscriptions_v1` (e.g. ETLs failed for multiple days and are catching up).
    OR month_start_date >= (
      SELECT
        DATE_ADD(MAX(month_start_date), INTERVAL 1 MONTH)
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_service_subscriptions_v1`
    )
  )
  AND month_start_date <= CURRENT_DATE() - 1
