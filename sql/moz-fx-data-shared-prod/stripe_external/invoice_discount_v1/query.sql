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
  subscription_id,
FROM
  `moz-fx-data-bq-fivetran`.stripe.invoice_discount
