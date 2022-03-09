CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn_derived.active_subscriptions`
AS
WITH json_arrays AS (
  SELECT
    *,
    TO_JSON_STRING(promotion_codes) AS json_promotion_codes
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.all_subscriptions_v1
)
SELECT
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
  JSON_VALUE_ARRAY(json_promotion_codes) AS promotion_codes,
  COUNT(*) AS `count`,
FROM
  json_arrays
CROSS JOIN
  UNNEST(GENERATE_DATE_ARRAY(DATE(subscription_start_date), DATE(end_date) - 1)) AS active_date
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
  json_promotion_codes
