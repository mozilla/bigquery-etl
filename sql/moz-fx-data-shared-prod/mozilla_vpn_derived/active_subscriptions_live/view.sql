CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn_derived.active_subscriptions_live`
AS
WITH all_subscriptions AS (
  SELECT
    *,
    TO_JSON_STRING(promotion_codes) AS json_promotion_codes
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn.all_subscriptions
)
SELECT
  active_subscription_ids.active_date,
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
  all_subscriptions.promotion_discounts_amount,
  COUNT(*) AS `count`,
FROM
  all_subscriptions
JOIN
  `moz-fx-data-shared-prod`.mozilla_vpn.active_subscription_ids
  USING (subscription_id)
GROUP BY
  active_date,
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
  json_promotion_codes,
  promotion_discounts_amount
