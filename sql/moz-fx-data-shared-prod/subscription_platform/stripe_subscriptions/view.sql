CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.stripe_subscriptions`
AS
WITH stripe_subscriptions AS (
  SELECT
    customer AS customer_id,
    id AS subscription_id,
    plan AS plan_id,
    status,
    event_timestamp,
    MIN(COALESCE(trial_end, start_date)) OVER (
      PARTITION BY
        customer
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING
    ) AS customer_start_date,
    COALESCE(trial_end, start_date) AS subscription_start_date,
    created,
    trial_end,
    canceled_at,
    mozfun.map.get_key(metadata, "cancelled_for_customer_at") AS canceled_for_customer_at,
    cancel_at,
    cancel_at_period_end,
    ended_at,
    discount.promotion_code AS promotion_code_id,
  FROM
    mozdata.stripe.subscriptions
  WHERE
    status NOT IN ("incomplete", "incomplete_expired")
),
stripe_customers AS (
  SELECT
    id AS customer_id,
    mozfun.map.get_key(metadata, "fxa_uid") AS fxa_uid,
  FROM
    mozdata.stripe.customers
),
stripe_charges AS (
  SELECT
    id AS charge_id,
    payment_method_details.card.country,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.charges_v1
  WHERE
    status = "succeeded"
),
stripe_invoice_lines AS (
  SELECT
    lines.subscription AS subscription_id,
    IF(
      "paypalTransactionId" IN (SELECT key FROM UNNEST(invoices_v1.metadata)),
      -- FxA copies paypal billing agreement location to customer_address
      STRUCT("Paypal" AS provider, invoices_v1.customer_address.country),
      ("Stripe", stripe_charges.country)
    ).*,
    invoices_v1.event_timestamp,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.invoices_v1
  CROSS JOIN
    UNNEST(lines) AS lines
  LEFT JOIN
    stripe_customers
  ON
    invoices_v1.customer = stripe_customers.customer_id
  LEFT JOIN
    stripe_charges
  ON
    invoices_v1.charge = stripe_charges.charge_id
),
stripe_promotion_codes AS (
  SELECT
    id AS promotion_code_id,
    code AS promotion_code,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.promotion_codes_v1
),
stripe_subscription_provider_country AS (
  SELECT
    subscription_id,
    ARRAY_AGG(
      STRUCT(provider, LOWER(country) AS country)
      ORDER BY
        -- prefer rows with country
        IF(country IS NULL, 0, 1) DESC,
        event_timestamp DESC
      LIMIT
        1
    )[OFFSET(0)].*
  FROM
    stripe_invoice_lines
  GROUP BY
    subscription_id
),
stripe_plans AS (
  SELECT
    plans.id AS plan_id,
    plans.amount AS plan_amount,
    plans.billing_scheme,
    plans.currency AS plan_currency,
    plans.interval AS plan_interval,
    plans.interval_count AS plan_interval_count,
    plans.product AS product_id,
    products.name AS product_name,
  FROM
    mozdata.stripe.plans
  LEFT JOIN
    mozdata.stripe.products
  ON
    plans.product = products.id
)
SELECT
  customer_id,
  subscription_id,
  plan_id,
  status,
  event_timestamp,
  customer_start_date,
  subscription_start_date,
  created,
  trial_end,
  canceled_at,
  canceled_for_customer_at,
  cancel_at,
  cancel_at_period_end,
  ended_at,
  fxa_uid,
  country,
  user_registration_date,
  entrypoint_experiment,
  entrypoint_variation,
  utm_campaign,
  utm_content,
  utm_medium,
  utm_source,
  utm_term,
  provider,
  plan_amount,
  IF(promotion_code IS NULL, [], [promotion_code]) AS promotion_codes,
FROM
  stripe_subscriptions
LEFT JOIN
  stripe_plans
USING
  (plan_id)
LEFT JOIN
  stripe_subscription_provider_country
USING
  (subscription_id)
LEFT JOIN
  stripe_customers
USING
  (customer_id)
LEFT JOIN
  stripe_promotion_codes
USING
  (promotion_code_id)
