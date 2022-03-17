CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.stripe_subscriptions`
AS
WITH subscription AS (
  SELECT
    customer_id,
    id AS subscription_id,
    status,
    _fivetran_synced,
    -- TODO verify logic for subscription_start_date
    COALESCE(trial_end, start_date) AS subscription_start_date,
    created,
    -- trial_start,
    trial_end,
    -- current_period_end,
    -- current_period_start,
    canceled_at,
    JSON_VALUE(metadata, "$.cancelled_for_customer_at") AS canceled_for_customer_at,
    cancel_at,
    cancel_at_period_end,
    IF(ended_at <= _fivetran_synced, ended_at, NULL) AS ended_at,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.subscription_history
  WHERE
    status NOT IN ("incomplete", "incomplete_expired")
    -- choose subscription from subscription history
    AND _fivetran_active
),
subscription_item AS (
  SELECT
    id AS subscription_item_id,
    subscription_id,
    plan_id,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.subscription_item
),
customer AS (
  SELECT
    id AS customer_id,
    TO_HEX(SHA256(JSON_VALUE(metadata, "$.userid"))) AS fxa_uid,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.customer
),
charge AS (
  SELECT
    charge.id AS charge_id,
    card.country,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.charge
  JOIN
    `moz-fx-data-bq-fivetran`.stripe.card
  ON
    charge.source_id = card.id
  WHERE
    charge.status = "succeeded"
),
invoice_provider_country AS (
  SELECT
    invoice_line_item.subscription_id,
    IF(
      JSON_VALUE(invoice.metadata, "$.paypalTransactionId") IS NOT NULL,
      -- FxA copies paypal billing agreement country to customer address
      STRUCT("Paypal" AS provider, customer.address_country AS country),
      ("Stripe", charge.country)
    ).*,
    invoice.finalized_at,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.invoice
  LEFT JOIN
    `moz-fx-data-bq-fivetran`.stripe.invoice_line_item
  ON
    invoice.id = invoice_line_item.invoice_id
  LEFT JOIN
    `moz-fx-data-bq-fivetran`.stripe.customer
  ON
    invoice.customer_id = customer.id
  LEFT JOIN
    charge
  USING
    (charge_id)
),
subscription_promotion_codes AS (
  SELECT
    subscription_discount.subscription_id,
    ARRAY_AGG(DISTINCT promotion_code.code IGNORE NULLS) AS promotion_codes,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.promotion_code
  JOIN
    `moz-fx-data-bq-fivetran`.stripe.subscription_discount
  USING
    (coupon_id, customer_id)
  GROUP BY
    subscription_id
),
subscription_provider_country AS (
  SELECT
    subscription_id,
    ARRAY_AGG(
      STRUCT(provider, LOWER(country) AS country)
      ORDER BY
        -- prefer rows with country
        IF(country IS NULL, 0, 1) DESC,
        finalized_at DESC
      LIMIT
        1
    )[OFFSET(0)].*
  FROM
    invoice_provider_country
  GROUP BY
    subscription_id
),
plan AS (
  SELECT
    plan.id AS plan_id,
    plan.amount AS plan_amount,
    plan.billing_scheme AS billing_scheme,
    plan.currency AS plan_currency,
    plan.interval AS plan_interval,
    plan.interval_count AS plan_interval_count,
    plan.product_id,
    product.name AS product_name,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.plan
  LEFT JOIN
    `moz-fx-data-bq-fivetran`.stripe.product
  ON
    plan.product_id = product.id
)
SELECT
  customer_id,
  subscription_id,
  subscription_item_id,
  plan_id,
  status,
  _fivetran_synced AS event_timestamp,
  MIN(subscription_start_date) OVER (PARTITION BY customer_id) AS customer_start_date,
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
  provider,
  plan_amount,
  billing_scheme,
  plan_currency,
  plan_interval,
  plan_interval_count,
  "Etc/UTC" AS plan_interval_timezone,
  product_id,
  product_name,
  promotion_codes,
FROM
  subscription
LEFT JOIN
  subscription_item
USING
  (subscription_id)
LEFT JOIN
  plan
USING
  (plan_id)
LEFT JOIN
  subscription_provider_country
USING
  (subscription_id)
LEFT JOIN
  customer
USING
  (customer_id)
LEFT JOIN
  subscription_promotion_codes
USING
  (subscription_id)
