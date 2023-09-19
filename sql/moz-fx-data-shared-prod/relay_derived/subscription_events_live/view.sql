CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.relay_derived.subscription_events_live`
AS
WITH subscriptions AS (
  SELECT
    *,
    TO_JSON_STRING(promotion_codes) AS json_promotion_codes
  FROM
    `moz-fx-data-shared-prod`.relay.subscriptions
),
max_active_date AS (
  SELECT AS VALUE
    MAX(active_date)
  FROM
    `moz-fx-data-shared-prod`.relay.active_subscription_ids
),
trials AS (
  SELECT
    *
  FROM
    subscriptions
  WHERE
    trial_start IS NOT NULL
    AND DATE(trial_start) <= (SELECT max_active_date FROM max_active_date)
),
new_trial_events AS (
  SELECT
    DATE(trial_start) AS event_date,
    subscription_id,
    "New Trial" AS event_type,
  FROM
    trials
),
cancelled_trial_events AS (
  SELECT
    DATE(ended_at) AS event_date,
    subscription_id,
    "Cancelled Trial" AS event_type,
  FROM
    trials
  WHERE
    subscription_start_date IS NULL
    AND ended_at IS NOT NULL
),
new_events AS (
  SELECT
    active_date AS event_date,
    subscription_id,
    "New" AS event_type,
  FROM
    `moz-fx-data-shared-prod`.relay.active_subscription_ids
  QUALIFY
    LAG(active_date) OVER (PARTITION BY subscription_id ORDER BY active_date) IS DISTINCT FROM (
      active_date - 1
    )
),
cancelled_events AS (
  SELECT
    active_date + 1 AS event_date,
    subscription_id,
    "Cancelled" AS event_type,
  FROM
    `moz-fx-data-shared-prod`.relay.active_subscription_ids
  CROSS JOIN
    max_active_date
  QUALIFY
    LEAD(active_date) OVER (PARTITION BY subscription_id ORDER BY active_date) IS DISTINCT FROM (
      active_date + 1
    )
    AND active_date < max_active_date
),
events AS (
  SELECT
    *
  FROM
    new_trial_events
  UNION ALL
  SELECT
    *
  FROM
    cancelled_trial_events
  UNION ALL
  SELECT
    *
  FROM
    new_events
  UNION ALL
  SELECT
    *
  FROM
    cancelled_events
)
SELECT
  events.event_date,
  events.event_type,
  CASE
    WHEN events.event_type IN ("New Trial", "Cancelled Trial")
      THEN events.event_type
    WHEN events.event_type = "New"
      THEN subscriptions.subscription_start_reason
    WHEN events.event_type = "Cancelled"
      THEN COALESCE(
          subscriptions.ended_reason,
          IF(subscriptions.provider = "Apple Store", "Cancelled by IAP", "Payment Failed")
        )
  END AS granular_event_type,
  subscriptions.plan_id,
  subscriptions.country,
  subscriptions.country_name,
  subscriptions.provider,
  subscriptions.plan_amount,
  subscriptions.plan_currency,
  subscriptions.plan_interval,
  subscriptions.plan_interval_count,
  subscriptions.product_id,
  subscriptions.product_name,
  subscriptions.pricing_plan,
  JSON_VALUE_ARRAY(subscriptions.json_promotion_codes) AS promotion_codes,
  COUNT(*) AS `count`,
FROM
  subscriptions
JOIN
  events
USING
  (subscription_id)
GROUP BY
  event_date,
  event_type,
  granular_event_type,
  plan_id,
  country,
  country_name,
  provider,
  plan_amount,
  plan_currency,
  plan_interval,
  plan_interval_count,
  product_id,
  product_name,
  pricing_plan,
  json_promotion_codes
