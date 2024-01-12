CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.relay_derived.active_subscriptions_live`
AS
WITH subscriptions AS (
  SELECT
    *,
    TO_JSON_STRING(promotion_codes) AS json_promotion_codes
  FROM
    `moz-fx-data-shared-prod`.relay.subscriptions
)
SELECT
  active_subscription_ids.active_date,
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
  subscriptions.promotion_discounts_amount,
  COUNT(*) AS `count`,
FROM
  subscriptions
JOIN
  `moz-fx-data-shared-prod`.relay.active_subscription_ids
  USING (subscription_id)
GROUP BY
  active_date,
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
  json_promotion_codes,
  promotion_discounts_amount
