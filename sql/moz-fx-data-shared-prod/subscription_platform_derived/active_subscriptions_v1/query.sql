WITH product_union AS (
  SELECT
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
    TO_JSON_STRING(promotion_codes) AS promotion_codes,
    promotion_discounts_amount,
    `count`
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn.active_subscriptions
  WHERE
    -- in order to avoid double counting, only include bundles via relay
    product_name NOT LIKE "%Relay%"
  UNION ALL
  SELECT
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
    TO_JSON_STRING(promotion_codes) AS promotion_codes,
    promotion_discounts_amount,
    `count`
  FROM
    `moz-fx-data-shared-prod`.relay.active_subscriptions
  UNION ALL
  SELECT
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
    TO_JSON_STRING(promotion_codes) AS promotion_codes,
    promotion_discounts_amount,
    `count`
  FROM
    `moz-fx-data-shared-prod`.hubs.active_subscriptions
)
SELECT
  * EXCEPT (`count`) REPLACE(JSON_VALUE_ARRAY(promotion_codes) AS promotion_codes),
  SUM(`count`) AS `count`,
FROM
  product_union
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
  product_union.promotion_codes,
  promotion_discounts_amount
