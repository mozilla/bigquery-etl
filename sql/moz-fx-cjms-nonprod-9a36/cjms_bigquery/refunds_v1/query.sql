SELECT
  refund.id AS refund_id,
  invoice.subscription_id,
  refund.amount,
  refund.created,
  refund.reason,
  refund.status,
FROM
  `moz-fx-data-shared-prod`.stripe_external.nonprod_refund_v1 AS refund
JOIN
  `moz-fx-data-shared-prod`.stripe_external.nonprod_charge_v1 AS charge
  ON refund.charge_id = charge.id
JOIN
  `moz-fx-data-shared-prod`.stripe_external.nonprod_invoice_v1 AS invoice
  ON charge.invoice_id = invoice.id
JOIN
  EXTERNAL_QUERY(
    "moz-fx-cjms-nonprod-9a36.us.cjms-sql",
    "SELECT subscription_id FROM subscriptions"
  )
  USING (subscription_id)
