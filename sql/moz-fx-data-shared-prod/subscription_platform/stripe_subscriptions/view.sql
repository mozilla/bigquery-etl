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
    COALESCE(
      TO_HEX(SHA256(JSON_VALUE(customer.metadata, "$.userid"))),
      JSON_VALUE(pre_fivetran_customer.metadata, "$.fxa_uid")
    ) AS fxa_uid,
    COALESCE(customer.address_country, pre_fivetran_customer.address_country) AS address_country,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.customer
  FULL JOIN
    -- include customers that were deleted before the initial fivetran stripe import
    `moz-fx-data-shared-prod`.stripe_external.pre_fivetran_customer
  USING
    (id)
),
charge AS (
  SELECT
    charge.id AS charge_id,
    COALESCE(card.country, charge.billing_detail_address_country) AS country,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.charge
  JOIN
    `moz-fx-data-bq-fivetran`.stripe.card
  ON
    charge.card_id = card.id
  WHERE
    charge.status = "succeeded"
),
invoice_provider_country AS (
  SELECT
    invoice.subscription_id,
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
    customer
  USING
    (customer_id)
  LEFT JOIN
    charge
  USING
    (charge_id)
  WHERE
    invoice.status = "paid"
),
subscription_promotion_codes AS (
  SELECT
    invoice.subscription_id,
    ARRAY_AGG(DISTINCT promotion_code.code IGNORE NULLS) AS promotion_codes,
  FROM
    `moz-fx-data-bq-fivetran`.stripe.invoice
  JOIN
    `moz-fx-data-bq-fivetran`.stripe.invoice_discount
  ON
    invoice.id = invoice_discount.invoice_id
  JOIN
    `moz-fx-data-bq-fivetran`.stripe.promotion_code
  ON
    invoice_discount.promotion_code = promotion_code.id
  WHERE
    invoice.status = "paid"
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
