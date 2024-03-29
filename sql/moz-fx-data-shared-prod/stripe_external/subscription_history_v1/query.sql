SELECT
  _fivetran_start,
  id,
  _fivetran_active,
  _fivetran_end,
  _fivetran_synced,
  application_fee_percent,
  billing,
  billing_cycle_anchor,
  billing_threshold_amount_gte,
  billing_threshold_reset_billing_cycle_anchor,
  cancel_at,
  cancel_at_period_end,
  canceled_at,
  created,
  current_period_end,
  current_period_start,
  customer_id,
  days_until_due,
  default_payment_method_id,
  default_source_id,
  ended_at,
  latest_invoice_id,
  metadata,
  pause_collection_behavior,
  pause_collection_resumes_at,
  pending_setup_intent_id,
  quantity,
  start,
  start_date,
  status,
  tax_percent,
  trial_end,
  trial_start,
  collection_method,
FROM
  `moz-fx-data-bq-fivetran`.stripe.subscription_history
