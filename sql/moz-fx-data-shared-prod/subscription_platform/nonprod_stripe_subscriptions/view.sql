CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.nonprod_stripe_subscriptions`
AS
WITH subscriptions AS (
  SELECT
    customer_id,
    id AS subscription_id,
    status,
    _fivetran_synced,
    COALESCE(trial_end, start_date) AS subscription_start_date,
    created,
    trial_start,
    trial_end,
    -- current_period_end,
    -- current_period_start,
    canceled_at,
    JSON_VALUE(metadata, "$.cancelled_for_customer_at") AS canceled_for_customer_at,
    cancel_at,
    cancel_at_period_end,
    ended_at,
  FROM
    `dev-fivetran`.stripe_nonprod.subscription_history
  WHERE
    status NOT IN ("incomplete", "incomplete_expired")
    -- choose subscription from subscription history
    AND _fivetran_active
),
subscription_items AS (
  SELECT
    id AS subscription_item_id,
    subscription_id,
    plan_id,
  FROM
    `dev-fivetran`.stripe_nonprod.subscription_item
),
customers AS (
  SELECT
    id AS customer_id,
    TO_HEX(SHA256(JSON_VALUE(metadata, "$.userid"))) AS fxa_uid,
    address_country,
  FROM
    `dev-fivetran`.stripe_nonprod.customer
),
charges AS (
  SELECT
    charges.id AS charge_id,
    COALESCE(cards.country, charges.billing_detail_address_country) AS country,
  FROM
    `dev-fivetran`.stripe_nonprod.charge AS charges
  JOIN
    `dev-fivetran`.stripe_nonprod.card AS cards
  ON
    charges.card_id = cards.id
  WHERE
    charges.status = "succeeded"
),
invoices_provider_country AS (
  SELECT
    invoices.subscription_id,
    IF(
      JSON_VALUE(invoices.metadata, "$.paypalTransactionId") IS NOT NULL,
      -- FxA copies paypal billing agreement country to customer address
      STRUCT("Paypal" AS provider, customers.address_country AS country),
      ("Stripe", charges.country)
    ).*,
    invoices.created,
  FROM
    `dev-fivetran`.stripe_nonprod.invoice AS invoices
  LEFT JOIN
    customers
  USING
    (customer_id)
  LEFT JOIN
    charges
  USING
    (charge_id)
  WHERE
    invoices.status = "paid"
),
subscriptions_promotions AS (
  SELECT
    invoices.subscription_id,
    ARRAY_AGG(DISTINCT promotion_codes.code IGNORE NULLS) AS promotion_codes,
    SUM(
      COALESCE(coupons.amount_off, 0) + COALESCE(
        CAST((invoices.subtotal * coupons.percent_off / 100) AS INT64),
        0
      )
    ) AS promotion_discounts_amount,
  FROM
    `dev-fivetran`.stripe_nonprod.invoice AS invoices
  JOIN
    `dev-fivetran`.stripe_nonprod.invoice_discount AS invoice_discounts
  ON
    invoices.id = invoice_discounts.invoice_id
  JOIN
    `dev-fivetran`.stripe_nonprod.promotion_code AS promotion_codes
  ON
    invoice_discounts.promotion_code = promotion_codes.id
  JOIN
    `dev-fivetran`.stripe_nonprod.coupon AS coupons
  ON
    promotion_codes.coupon_id = coupons.id
  WHERE
    invoices.status = "paid"
  GROUP BY
    subscription_id
),
subscriptions_provider_country AS (
  SELECT
    subscription_id,
    ARRAY_AGG(
      STRUCT(provider, LOWER(country) AS country)
      ORDER BY
        -- prefer rows with country
        IF(country IS NULL, 0, 1) DESC,
        created DESC
      LIMIT
        1
    )[OFFSET(0)].*
  FROM
    invoices_provider_country
  GROUP BY
    subscription_id
),
plans AS (
  SELECT
    plans.id AS plan_id,
    plans.amount AS plan_amount,
    plans.billing_scheme AS billing_scheme,
    plans.currency AS plan_currency,
    plans.interval AS plan_interval,
    plans.interval_count AS plan_interval_count,
    plans.product_id,
    products.name AS product_name,
  FROM
    `dev-fivetran`.stripe_nonprod.plan AS plans
  LEFT JOIN
    `dev-fivetran`.stripe_nonprod.product AS products
  ON
    plans.product_id = products.id
)
SELECT
  subscriptions.customer_id,
  subscriptions.subscription_id,
  subscription_items.subscription_item_id,
  subscription_items.plan_id,
  subscriptions.status,
  subscriptions._fivetran_synced AS event_timestamp,
  subscriptions.subscription_start_date,
  subscriptions.created,
  subscriptions.trial_start,
  subscriptions.trial_end,
  subscriptions.canceled_at,
  subscriptions.canceled_for_customer_at,
  subscriptions.cancel_at,
  subscriptions.cancel_at_period_end,
  subscriptions.ended_at,
  customers.fxa_uid,
  subscriptions_provider_country.country,
  subscriptions_provider_country.provider,
  plans.plan_amount,
  plans.billing_scheme,
  plans.plan_currency,
  plans.plan_interval,
  plans.plan_interval_count,
  "Etc/UTC" AS plan_interval_timezone,
  plans.product_id,
  plans.product_name,
  subscriptions_promotions.promotion_codes,
  subscriptions_promotions.promotion_discounts_amount,
FROM
  subscriptions
LEFT JOIN
  subscription_items
USING
  (subscription_id)
LEFT JOIN
  plans
USING
  (plan_id)
LEFT JOIN
  subscriptions_provider_country
USING
  (subscription_id)
LEFT JOIN
  customers
USING
  (customer_id)
LEFT JOIN
  subscriptions_promotions
USING
  (subscription_id)
