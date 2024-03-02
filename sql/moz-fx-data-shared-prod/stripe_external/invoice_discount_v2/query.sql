WITH new_invoice_discounts AS (
  SELECT
    id,
    invoice_id,
    _fivetran_synced,
    checkout_session_id AS checkout_session,
    coupon_id,
    customer_id,
    `end`,
    CAST(NULL AS STRING) AS invoice_item_id,
    promotion_code,
    start,
    subscription_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.discount_v1`
  WHERE
    type = 'PARENT_INVOICE_LINE_ITEM'
  QUALIFY
    -- Select one discount record per invoice to match the grain of `invoice_discount_v1`
    -- so this can be a straightforward replacement for where that's been used.
    1 = ROW_NUMBER() OVER (
      PARTITION BY
        invoice_id
      ORDER BY
        -- Prefer normal subscription line items over those associated with an invoice item (e.g. prorations).
        IF(invoice_item_id IS NULL, 1, 2),
        start DESC,
        id,
        type_id
    )
),
old_invoice_discounts AS (
  SELECT
    id,
    invoice_id,
    _fivetran_synced,
    checkout_session,
    coupon_id,
    customer_id,
    `end`,
    invoice_item_id,
    promotion_code,
    start,
    subscription_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.invoice_discount_v1`
),
invoices_synced_after_new_discounts AS (
  SELECT
    id AS invoice_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.invoice_v1`
  WHERE
    -- Fivetran began syncing the new `discount` table on 2024-02-28.
    -- See https://mozilla-hub.atlassian.net/browse/DENG-2116?focusedCommentId=842827.
    DATE(_fivetran_synced) >= '2024-02-28'
)
SELECT
  *
FROM
  new_invoice_discounts
UNION ALL
SELECT
  old_invoice_discounts.*
FROM
  old_invoice_discounts
LEFT JOIN
  new_invoice_discounts
  USING (invoice_id)
LEFT JOIN
  invoices_synced_after_new_discounts
  USING (invoice_id)
WHERE
  new_invoice_discounts.invoice_id IS NULL
  -- We don't include old discounts for invoices synced after the new `discount` table began syncing
  -- because the new `discount` table should have their discounts, if any (Fivetran hard deletes discounts).
  AND invoices_synced_after_new_discounts.invoice_id IS NULL
