CREATE OR REPLACE TABLE
  `moz-fx-cjms-nonprod-9a36`.cjms_bigquery.refund_v1
AS
SELECT
  refund.id AS refund_id,
  invoice.subscription_id,
  refund.amount,
  refund.created,
  refund.reason,
  refund.status,
FROM
  `dev-fivetran`.stripe_nonprod.refund
JOIN
  `dev-fivetran`.stripe_nonprod.charge
ON
  refund.charge_id = charge.id
JOIN
  `dev-fivetran`.stripe_nonprod.invoice
ON
  charge.invoice_id = invoice.id
JOIN
  EXTERNAL_QUERY(
    "moz-fx-cjms-nonprod-9a36.us.cjms-sql",
    "SELECT subscription_id FROM subscriptions"
  )
USING
  (subscription_id)
