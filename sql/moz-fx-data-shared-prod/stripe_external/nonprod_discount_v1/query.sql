SELECT
  id,
  type,
  type_id,
  _fivetran_synced,
  amount,
  checkout_session_id,
  checkout_session_line_item_id,
  coupon_id,
  credit_note_line_item_id,
  customer_id,
  `end`,
  invoice_id,
  invoice_item_id,
  promotion_code,
  start,
  subscription_id,
FROM
  `dev-fivetran.stripe_nonprod.discount`
WHERE
  -- Fivetran used to have a bug where it synced subscription discounts as customer discounts.
  NOT (type = 'CUSTOMER' AND subscription_id IS NOT NULL)
