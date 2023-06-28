CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stripe.itemized_payout_reconciliation`
AS
WITH customers_without_shipping_state AS (
  SELECT
    id AS customer_id,
    NULLIF(address_country, "") AS country,
    NULLIF(UPPER(TRIM(address_postal_code)), "") AS postal_code,
    NULLIF(address_state, "") AS state,
    NULLIF(shipping_address_country, "") AS shipping_address_country,
    NULLIF(UPPER(TRIM(shipping_address_postal_code)), "") AS shipping_address_postal_code,
    NULLIF(shipping_address_state, "") AS shipping_address_state,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.customer_v1
),
postal_code_to_state AS (
  SELECT
    country,
    postal_code,
    IF(COUNT(DISTINCT state) > 1, NULL, ANY_VALUE(state)) AS state,
  FROM
    customers_without_shipping_state
  WHERE
    country IN ("US", "CA")
    AND postal_code IS NOT NULL
    AND state IS NOT NULL
    AND LENGTH(state) = 2
  GROUP BY
    country,
    postal_code
),
customers AS (
  SELECT
    customers.* REPLACE (
      COALESCE(
        customers.shipping_address_state,
        postal_code_to_state.state
      ) AS shipping_address_state
    )
  FROM
    customers_without_shipping_state AS customers
  LEFT JOIN
    postal_code_to_state
  ON
    customers.shipping_address_country = postal_code_to_state.country
    AND customers.shipping_address_postal_code = postal_code_to_state.postal_code
),
charge_states AS (
  SELECT
    charges.id AS charge_id,
    cards.country AS card_country,
    NULLIF(UPPER(TRIM(charges.billing_detail_address_postal_code)), "") AS postal_code,
    postal_code_to_state.state,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.charge_v1 AS charges
  JOIN
    `moz-fx-data-shared-prod`.stripe_external.card_v1 AS cards
  ON
    charges.card_id = cards.id
  JOIN
    postal_code_to_state
  ON
    cards.country = postal_code_to_state.country
    AND UPPER(TRIM(charges.billing_detail_address_postal_code)) = postal_code_to_state.postal_code
  WHERE
    cards.country IN ("US", "CA")
),
subscriptions AS (
  SELECT
    subscription_id,
    plan_id,
    plan_name,
    plan_interval,
    plan_interval_count,
    product_id,
    product_name,
  FROM
    `moz-fx-data-shared-prod`.subscription_platform.stripe_subscriptions
),
enriched AS (
  SELECT
    report.* EXCEPT (
      card_country,
      shipping_address_city,
      shipping_address_country,
      shipping_address_line1,
      shipping_address_line2,
      shipping_address_postal_code,
      shipping_address_state
    ),
    {% for country, state, postal_code, dst_pre in [
        (
          "customers.shipping_address_country",
          "customers.shipping_address_state",
          "customers.shipping_address_postal_code",
          "shipping_address"
        ),
        ("customers.country", "customers.state", "customers.postal_code", "address"),
        ("card_country", "charge_states.state", "charge_states.postal_code", "card"),
    ] -%}
      CASE
        -- American Samoa
        WHEN `{{country}}` = "US"
          AND REGEXP_CONTAINS({{postal_code}}, "^96799(-?[0-9]{4})?$")
          THEN STRUCT("AS" AS `{{dst_pre}}_country`, NULL AS `{{dst_pre}}_state`)
        -- Puerto Rico
        WHEN `{{country}}` = "US"
          AND REGEXP_CONTAINS({{postal_code}}, "^00[679][0-9]{2}(-?[0-9]{4})?$")
          THEN STRUCT("PR" AS `{{dst_pre}}_country`, NULL AS `{{dst_pre}}_state`)
        -- Virgin Islands
        WHEN `{{country}}` = "US"
          AND REGEXP_CONTAINS({{postal_code}}, "^008[0-9]{2}(-?[0-9]{4})?$")
          THEN STRUCT("VI" AS `{{dst_pre}} country`, NULL AS `{{dst_pre}}_state`)
        ELSE STRUCT({{country}} AS `{{dst_pre}}_country`, {{state}} AS `{{dst_pre}}_state`)
      END.*,
      {{postal_code}} AS `{{dst_pre}}_postal_code`,
    {% endfor -%}
    subscriptions.* EXCEPT (subscription_id),
  FROM
    `moz-fx-data-shared-prod`.stripe_external.itemized_payout_reconciliation_v5 AS report
  LEFT JOIN
    charge_states
  USING
    (charge_id, card_country)
  LEFT JOIN
    customers
  USING
    (customer_id)
  LEFT JOIN
    subscriptions
  USING
    (subscription_id)
)
SELECT
  *,
  -- Use the same address hierarchy as Stripe Tax after we enabled Stripe Tax (FXA-5457).
  -- https://stripe.com/docs/tax/customer-locations#address-hierarchy
  -- (customer shipping address, customer billing address, payment method billing address)
  CASE
    WHEN shipping_address_country IS NOT NULL
      THEN STRUCT(
          shipping_address_country AS tax_country,
          shipping_address_state AS tax_state,
          shipping_address_postal_code AS tax_postal_code
        )
    WHEN address_country IS NOT NULL
      THEN STRUCT(
          address_country AS tax_country,
          address_state AS tax_state,
          address_postal_code AS tax_postal_code
        )
    WHEN card_country IS NOT NULL
      THEN STRUCT(
          card_country AS tax_country,
          card_state AS tax_state,
          card_postal_code AS tax_postal_code
        )
  END.*,
FROM
  enriched
