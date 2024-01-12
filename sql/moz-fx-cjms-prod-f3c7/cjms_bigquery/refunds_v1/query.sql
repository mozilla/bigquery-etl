SELECT
  refund.id AS refund_id,
  invoice.subscription_id,
  refund.amount,
  refund.created,
  refund.reason,
  refund.status,
FROM
  `moz-fx-data-shared-prod`.stripe_external.refund_v1 AS refund
JOIN
  `moz-fx-data-shared-prod`.stripe_external.charge_v1 AS charge
  ON refund.charge_id = charge.id
JOIN
  `moz-fx-data-shared-prod`.stripe_external.invoice_v1 AS invoice
  ON charge.invoice_id = invoice.id
JOIN
  EXTERNAL_QUERY("moz-fx-cjms-prod-f3c7.us.cjms-sql", "SELECT subscription_id FROM subscriptions")
  USING (subscription_id)
