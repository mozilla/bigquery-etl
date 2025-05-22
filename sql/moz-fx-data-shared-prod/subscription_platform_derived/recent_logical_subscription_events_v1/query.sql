SELECT
  -- Match the deployed `logical_subscription_events_v1` table schema so this table can be easily unioned with it.
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
    ) AS subscription,
    (
      SELECT AS STRUCT
        old_subscription.* EXCEPT (
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
        old_subscription.initial_discount_name,
        old_subscription.initial_discount_promotion_code,
        old_subscription.current_period_discount_name,
        old_subscription.current_period_discount_promotion_code,
        old_subscription.current_period_discount_amount,
        old_subscription.ongoing_discount_name,
        old_subscription.ongoing_discount_promotion_code,
        old_subscription.ongoing_discount_amount,
        old_subscription.ongoing_discount_ends_at
    ) AS old_subscription
  )
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.logical_subscription_events_v1_live`
WHERE
  (
    DATE(`timestamp`) >= CURRENT_DATE() - 7
    -- Include more than the previous 7 days if necessary to avoid data gaps between this table and
    -- `logical_subscription_events_v1` (e.g. ETLs failed for multiple days and are catching up).
    OR DATE(`timestamp`) >= (
      SELECT
        MAX(DATE(`timestamp`)) + 1
      FROM
        `moz-fx-data-shared-prod.subscription_platform_derived.logical_subscription_events_v1`
    )
  )
  AND DATE(`timestamp`) <= CURRENT_DATE() - 1
