CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn_derived.subscription_events_live`
AS
WITH all_subscriptions AS (
  SELECT
    *,
    TO_JSON_STRING(promotion_codes) AS json_promotion_codes
  FROM
    mozdata.mozilla_vpn.all_subscriptions
),
previous_active_subscription_ids AS (
  SELECT
    active_date + 1 AS event_date,
    subscription_id,
  FROM
    mozdata.mozilla_vpn.active_subscription_ids
),
current_active_subscription_ids AS (
  SELECT
    active_date AS event_date,
    subscription_id,
  FROM
    mozdata.mozilla_vpn.active_subscription_ids
),
events AS (
  SELECT
    event_date,
    subscription_id,
    IF(previous_active_subscription_ids.subscription_id IS NULL, "New", "Cancelled") AS event_type,
  FROM
    current_active_subscription_ids
  FULL JOIN
    previous_active_subscription_ids
  USING
    (event_date, subscription_id)
  WHERE
    previous_active_subscription_ids.subscription_id IS NULL
    OR current_active_subscription_ids IS NULL
)
SELECT
  events.event_date,
  events.event_type,
  CASE
  -- "New" events
  WHEN
    events.event_type = "New"
    AND DATE(all_subscriptions.subscription_start_date) = DATE(
      all_subscriptions.customer_start_date
    )
  THEN
    "New"
  WHEN
    events.event_type = "New"
  THEN
    "Resurrected"
  -- "Cancelled" events
  WHEN
    all_subscriptions.provider LIKE "Apple Store IAP"
  THEN
    "Cancelled by IAP"
  WHEN
    all_subscriptions.canceled_for_customer_at IS NOT NULL
    OR all_subscriptions.cancel_at_period_end
  THEN
    "Cancelled by Customer"
  ELSE
    "Payment Failed"
  END
  AS granular_event_type,
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
  SUM(IF(event_type = "New", 1, -1)) AS `count`,
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
