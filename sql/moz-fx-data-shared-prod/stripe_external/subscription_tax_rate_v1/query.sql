SELECT
  subscription_id,
  tax_rate_id,
  _fivetran_synced,
FROM
  `moz-fx-data-bq-fivetran`.stripe.subscription_tax_rate
