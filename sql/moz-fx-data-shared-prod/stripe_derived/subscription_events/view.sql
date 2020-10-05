CREATE OR REPLACE VIEW
  subscription_events
AS
WITH raw AS (
  SELECT
    created AS event_timestamp,
    `data`.subscription.*,
  FROM
    stripe_external.events_v1
  WHERE
    `data`.subscription IS NOT NULL
  UNION ALL
  SELECT
    created AS event_timestamp,
    *,
  FROM
    stripe_external.subscriptions_v1
)
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
  raw
