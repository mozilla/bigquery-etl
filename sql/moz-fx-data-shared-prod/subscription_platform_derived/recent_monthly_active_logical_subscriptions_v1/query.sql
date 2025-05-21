SELECT
  -- Match the deployed `monthly_active_logical_subscriptions_v1` table schema so this table can be easily unioned with it.
  * REPLACE (
    (
      SELECT AS STRUCT
        subscription.* EXCEPT (
          initial_discount_name,
          initial_discount_promotion_code,
          current_period_discount_name,
          current_period_discount_promotion_code,
          current_period_discount_amount,
          ongoing_discount_name,
          ongoing_discount_promotion_code,
          ongoing_discount_amount,
          ongoing_discount_ends_at
        ),
        subscription.initial_discount_name,
        subscription.initial_discount_promotion_code,
        subscription.current_period_discount_name,
        subscription.current_period_discount_promotion_code,
        subscription.current_period_discount_amount,
        subscription.ongoing_discount_name,
        subscription.ongoing_discount_promotion_code,
        subscription.ongoing_discount_amount,
        subscription.ongoing_discount_ends_at
    ) AS subscription
  )
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_logical_subscriptions_v1_live`
WHERE
  (
    month_start_date >= CURRENT_DATE() - 7
    -- Include more than the previous 7 days if necessary to avoid data gaps between this table and
    -- `monthly_active_logical_subscriptions_v1` (e.g. ETLs failed for multiple days and are catching up).
    OR month_start_date >= (
      SELECT
        DATE_ADD(MAX(month_start_date), INTERVAL 1 MONTH)
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_logical_subscriptions_v1`
    )
  )
  AND month_start_date <= CURRENT_DATE() - 1
