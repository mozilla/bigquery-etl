SELECT
  id,
  subscription_id,
  _fivetran_synced,
  billing_thresholds_amount_gte,
  billing_thresholds_reset_billing_cycle_anchor,
  created,
  metadata,
  plan_id,
  quantity,
FROM
  `dev-fivetran`.stripe_nonprod.subscription_item
