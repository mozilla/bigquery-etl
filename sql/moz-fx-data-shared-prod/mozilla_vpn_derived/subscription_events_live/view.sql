CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn_derived.subscription_events_live`
AS
WITH all_subscriptions AS (
  SELECT
    *,
    TO_JSON_STRING(promotion_codes) AS json_promotion_codes
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn.all_subscriptions
),
max_active_date AS (
  SELECT AS VALUE
    MAX(active_date)
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn.active_subscription_ids
),
trials AS (
  SELECT
    *
  FROM
    all_subscriptions
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
    `moz-fx-data-shared-prod`.mozilla_vpn.active_subscription_ids
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
    `moz-fx-data-shared-prod`.mozilla_vpn.active_subscription_ids
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
      THEN all_subscriptions.subscription_start_reason
    WHEN events.event_type = "Cancelled"
      THEN COALESCE(
          all_subscriptions.ended_reason,
          IF(all_subscriptions.provider = "Apple Store", "Cancelled by IAP", "Payment Failed")
        )
  END AS granular_event_type,
  all_subscriptions.plan_id,
  all_subscriptions.status,
  all_subscriptions.country,
  all_subscriptions.country_name,
  all_subscriptions.entrypoint_experiment,
  all_subscriptions.entrypoint_variation,
  all_subscriptions.utm_campaign,
  all_subscriptions.utm_content,
  all_subscriptions.utm_medium,
  all_subscriptions.utm_source,
  all_subscriptions.utm_term,
  all_subscriptions.provider,
  all_subscriptions.plan_amount,
  all_subscriptions.billing_scheme,
  all_subscriptions.plan_currency,
  all_subscriptions.plan_interval,
  all_subscriptions.plan_interval_count,
  all_subscriptions.product_id,
  all_subscriptions.product_name,
  all_subscriptions.pricing_plan,
  all_subscriptions.normalized_acquisition_channel,
  all_subscriptions.normalized_campaign,
  all_subscriptions.normalized_content,
  all_subscriptions.normalized_medium,
  all_subscriptions.normalized_source,
  all_subscriptions.website_channel_group,
  JSON_VALUE_ARRAY(all_subscriptions.json_promotion_codes) AS promotion_codes,
  COUNT(*) AS `count`,
FROM
  all_subscriptions
JOIN
  events
USING
  (subscription_id)
GROUP BY
  event_date,
  event_type,
  granular_event_type,
  plan_id,
  status,
  country,
  country_name,
  entrypoint_experiment,
  entrypoint_variation,
  utm_campaign,
  utm_content,
  utm_medium,
  utm_source,
  utm_term,
  provider,
  plan_amount,
  billing_scheme,
  plan_currency,
  plan_interval,
  plan_interval_count,
  product_id,
  product_name,
  pricing_plan,
  normalized_acquisition_channel,
  normalized_campaign,
  normalized_content,
  normalized_medium,
  normalized_source,
  website_channel_group,
  json_promotion_codes
