WITH new_customer_discounts AS (
  SELECT
    customer_id,
    id,
    _fivetran_synced,
    checkout_session_id AS checkout_session,
    coupon_id,
    `end`,
    invoice_id,
    invoice_item_id,
    promotion_code,
    start,
    subscription_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.discount_v1`
  WHERE
    type = 'CUSTOMER'
),
old_customer_discounts AS (
  SELECT
    customer_id,
    id,
    _fivetran_synced,
    checkout_session,
    coupon_id,
    `end`,
    invoice_id,
    invoice_item_id,
    promotion_code,
    start,
    subscription_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.customer_discount_v1`
),
customers_synced_after_new_discounts AS (
  SELECT
    id AS customer_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.customer_v1`
  WHERE
    -- Fivetran began syncing the new `discount` table on 2024-02-28.
    -- See https://mozilla-hub.atlassian.net/browse/DENG-2116?focusedCommentId=842827.
    DATE(_fivetran_synced) >= '2024-02-28'
)
SELECT
  *
FROM
  new_customer_discounts
UNION ALL
SELECT
  old_customer_discounts.*
FROM
  old_customer_discounts
LEFT JOIN
  new_customer_discounts
  USING (customer_id)
LEFT JOIN
  customers_synced_after_new_discounts
  USING (customer_id)
WHERE
  new_customer_discounts.customer_id IS NULL
  -- We don't include old discounts for customers synced after the new `discount` table began syncing
  -- because the new `discount` table should have their discounts, if any (Fivetran hard deletes discounts).
  AND customers_synced_after_new_discounts.customer_id IS NULL
