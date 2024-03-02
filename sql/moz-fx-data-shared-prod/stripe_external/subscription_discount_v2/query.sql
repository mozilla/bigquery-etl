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
subscriptions_synced_after_new_discounts AS (
  SELECT DISTINCT
    id AS subscription_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.subscription_history_v1`
  WHERE
    -- Fivetran began syncing the new `discount` table on 2024-02-28.
    -- See https://mozilla-hub.atlassian.net/browse/DENG-2116?focusedCommentId=842827.
    DATE(_fivetran_synced) >= '2024-02-28'
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
  subscriptions_synced_after_new_discounts
  USING (subscription_id)
WHERE
  new_subscription_discounts.subscription_id IS NULL
  -- We don't include old discounts for subscriptions synced after the new `discount` table began syncing
  -- because the new `discount` table should have their discounts, if any (Fivetran hard deletes discounts).
  AND subscriptions_synced_after_new_discounts.subscription_id IS NULL
