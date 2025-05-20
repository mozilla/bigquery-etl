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
  (month_start_date BETWEEN DATE_TRUNC((CURRENT_DATE() - 7), MONTH) AND (CURRENT_DATE() - 1))
