WITH new_subscription_discounts AS (
  SELECT
    id,
    subscription_id,
    _fivetran_synced,
    checkout_session_id AS checkout_session,
    coupon_id,
    customer_id,
    `end`,
    invoice_id,
    invoice_item_id,
    promotion_code,
    start
  FROM
    `moz-fx-data-shared-prod.stripe_external.discount_v1`
  WHERE
    type = 'SUBSCRIPTION'
),
old_subscription_discounts AS (
  SELECT
    id,
    subscription_id,
    _fivetran_synced,
    checkout_session,
    coupon_id,
    customer_id,
    `end`,
    invoice_id,
    invoice_item_id,
    promotion_code,
    start
  FROM
    `moz-fx-data-shared-prod.stripe_external.subscription_discount_v1`
),
subscriptions_synced_after_new_discounts_cutover AS (
  SELECT DISTINCT
    id AS subscription_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.subscription_history_v1`
  WHERE
    DATE(_fivetran_synced) >= '2024-10-23'
)
SELECT
  *
FROM
  new_subscription_discounts
UNION ALL
SELECT
  old_subscription_discounts.*
FROM
  old_subscription_discounts
LEFT JOIN
  new_subscription_discounts
  USING (subscription_id)
LEFT JOIN
  subscriptions_synced_after_new_discounts_cutover
  USING (subscription_id)
WHERE
  new_subscription_discounts.subscription_id IS NULL
  -- We don't include old discounts for subscriptions synced after cutting over to the new `discount` table
  -- because the new `discount` table should have their discounts, if any (Fivetran hard deletes discounts).
  AND subscriptions_synced_after_new_discounts_cutover.subscription_id IS NULL
