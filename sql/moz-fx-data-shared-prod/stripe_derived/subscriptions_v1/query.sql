SELECT
  -- limit fields in stripe_derived so as not to expose sensitive data
  created,
  cancel_at,
  cancel_at_period_end,
  canceled_at,
  customer,
  ended_at,
  event_timestamp,
  id,
  metadata,
  plan.id AS plan,
  start_date,
  status,
  trial_end,
FROM
  stripe_external.subscriptions_v1
