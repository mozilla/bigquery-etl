SELECT
  id,
  _fivetran_synced,
  active,
  created,
  description,
  display_name,
  inclusive,
  jurisdiction,
  metadata,
  percentage,
FROM
  `moz-fx-data-bq-fivetran`.stripe.tax_rate
