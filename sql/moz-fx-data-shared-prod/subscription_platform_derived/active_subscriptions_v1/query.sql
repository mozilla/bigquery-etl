{#- format off #}
WITH product_union AS (
{%- for product in ["mozilla_vpn", "relay", "hubs"] %}
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
    `moz-fx-data-shared-prod`.{{product}}.active_subscriptions
{%- if product == "mozilla_vpn" %}
  WHERE
    -- in order to avoid double counting, only include bundles via relay
    product_name NOT LIKE "%Relay%"
{%- endif %}
{%- if not loop.last %}
  UNION ALL
{%- endif %}
{%- endfor %}
{#- format on #}
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
