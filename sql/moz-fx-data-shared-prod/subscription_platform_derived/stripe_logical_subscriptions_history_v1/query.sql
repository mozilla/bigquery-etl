CREATE TEMP FUNCTION calculate_discount_amount(
  amount_off INTEGER,
  percent_off FLOAT64,
  original_amount INTEGER
)
RETURNS DECIMAL AS (
  CAST(COALESCE(amount_off, ROUND(original_amount * percent_off / 100), 0) AS DECIMAL) / 100
);

WITH subscriptions_history AS (
  SELECT
    id,
    valid_from,
    valid_to,
    subscription,
    -- This should be kept in agreement with what SubPlat considers an active Stripe subscription.
    -- https://github.com/mozilla/fxa/blob/56026cd08e60525823c60c4f4116f705e79d6124/packages/fxa-shared/subscriptions/stripe.ts#L19-L24
    subscription.status IN ('active', 'past_due', 'trialing') AS subscription_is_active
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_history_v2`
),
active_subscriptions_history AS (
  -- Only include a subscription's history once it becomes active.
  SELECT
    *,
    FIRST_VALUE(
      IF(subscription_is_active, valid_from, NULL) IGNORE NULLS
    ) OVER subscription_history_to_date_asc AS subscription_first_active_at
  FROM
    subscriptions_history
  QUALIFY
    LOGICAL_OR(subscription_is_active) OVER subscription_history_to_date_asc
  WINDOW
    subscription_history_to_date_asc AS (
      PARTITION BY
        subscription.id
      ORDER BY
        valid_from
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND CURRENT ROW
    )
),
plan_services AS (
  SELECT
    plan_id,
    ARRAY_AGG(
      STRUCT(services.id, services.name, tier.name AS tier)
      ORDER BY
        services.id
    ) AS services
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.services_v1` AS services
  CROSS JOIN
    UNNEST(services.stripe_plan_ids) AS plan_id
  LEFT JOIN
    UNNEST(services.tiers) AS tier
    ON plan_id IN UNNEST(tier.stripe_plan_ids)
  GROUP BY
    plan_id
),
paypal_subscriptions AS (
  SELECT DISTINCT
    subscription_id
  FROM
    `moz-fx-data-shared-prod.stripe_external.invoice_v1`
  WHERE
    JSON_VALUE(metadata, '$.paypalTransactionId') IS NOT NULL
),
subscriptions_history_charge_summaries AS (
  SELECT
    history.id AS subscriptions_history_id,
    ARRAY_AGG(
      cards.country IGNORE NULLS
      ORDER BY
        -- Prefer charges that succeeded.
        IF(charges.status = 'succeeded', 1, 2),
        charges.created DESC
      LIMIT
        1
    )[SAFE_ORDINAL(1)] AS latest_card_country,
    LOGICAL_OR(refunds.status = 'succeeded') AS has_refunds,
    LOGICAL_OR(
      charges.fraud_details_user_report = 'fraudulent'
      OR (
        charges.fraud_details_stripe_report = 'fraudulent'
        AND charges.fraud_details_user_report IS DISTINCT FROM 'safe'
      )
      OR (refunds.reason = 'fraudulent' AND refunds.status = 'succeeded')
    ) AS has_fraudulent_charges
  FROM
    `moz-fx-data-shared-prod.stripe_external.charge_v1` AS charges
  JOIN
    `moz-fx-data-shared-prod.stripe_external.invoice_v1` AS invoices
    ON charges.invoice_id = invoices.id
  JOIN
    active_subscriptions_history AS history
    ON invoices.subscription_id = history.subscription.id
    AND charges.created < history.valid_to
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.card_v1` AS cards
    ON charges.card_id = cards.id
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.refund_v1` AS refunds
    ON charges.id = refunds.charge_id
  GROUP BY
    subscriptions_history_id
),
subscription_period_discounts AS (
  SELECT
    invoice_line_items.subscription_id,
    invoice_line_items.period_start,
    invoice_line_items.period_end,
    ARRAY_AGG(
      STRUCT(
        invoices.created AS invoice_created_at,
        coupons.name AS coupon_name,
        promotion_codes.code AS promotion_code,
        coupons.amount_off,
        coupons.percent_off
      )
      ORDER BY
        invoices.created,
        invoice_line_items.id
      LIMIT
        1
    )[ORDINAL(1)].*
  FROM
    `moz-fx-data-shared-prod.stripe_external.invoice_v1` AS invoices
  JOIN
    `moz-fx-data-shared-prod.stripe_external.invoice_line_item_v1` AS invoice_line_items
    ON invoices.id = invoice_line_items.invoice_id
    AND invoice_line_items.type = 'subscription'
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.invoice_discount_v2` AS invoice_discounts
    ON invoices.id = invoice_discounts.invoice_id
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.coupon_v1` AS coupons
    ON invoice_discounts.coupon_id = coupons.id
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.promotion_code_v1` AS promotion_codes
    ON invoice_discounts.promotion_code = promotion_codes.id
  GROUP BY
    invoice_line_items.subscription_id,
    invoice_line_items.period_start,
    invoice_line_items.period_end
),
subscription_initial_discounts AS (
  SELECT
    *
  FROM
    subscription_period_discounts
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY invoice_created_at)
),
subscriptions_history_ongoing_discounts AS (
  SELECT
    history.id AS subscriptions_history_id,
    (
      CASE
        WHEN history.subscription.discount.coupon.duration = 'forever'
          OR history.subscription.discount.`end` > history.subscription.current_period_end
          THEN STRUCT(
              history.subscription.discount.coupon.name AS coupon_name,
              subscription_promotion_codes.code AS promotion_code,
              history.subscription.discount.coupon.amount_off,
              history.subscription.discount.coupon.percent_off,
              history.subscription.discount.`end` AS ends_at
            )
        WHEN history.subscription.customer.discount.coupon.duration = 'forever'
          OR history.subscription.customer.discount.`end` > history.subscription.current_period_end
          THEN STRUCT(
              history.subscription.customer.discount.coupon.name AS coupon_name,
              customer_promotion_codes.code AS promotion_code,
              history.subscription.customer.discount.coupon.amount_off,
              history.subscription.customer.discount.coupon.percent_off,
              history.subscription.customer.discount.`end` AS ends_at
            )
        ELSE NULL
      END
    ).*
  FROM
    active_subscriptions_history AS history
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.promotion_code_v1` AS subscription_promotion_codes
    ON history.subscription.discount.promotion_code_id = subscription_promotion_codes.id
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.promotion_code_v1` AS customer_promotion_codes
    ON history.subscription.customer.discount.promotion_code_id = customer_promotion_codes.id
)
SELECT
  CONCAT(
    'Stripe-',
    history.subscription.id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', history.subscription_first_active_at),
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', history.valid_from)
  ) AS id,
  history.valid_from,
  history.valid_to,
  history.id AS provider_subscriptions_history_id,
  STRUCT(
    CONCAT(
      'Stripe-',
      history.subscription.id,
      '-',
      FORMAT_TIMESTAMP('%FT%H:%M:%E6S', history.subscription_first_active_at)
    ) AS id,
    'Stripe' AS provider,
    IF(paypal_subscriptions.subscription_id IS NOT NULL, 'PayPal', 'Stripe') AS payment_provider,
    history.subscription.id AS provider_subscription_id,
    subscription_item.id AS provider_subscription_item_id,
    history.subscription.created AS provider_subscription_created_at,
    history.subscription.customer.id AS provider_customer_id,
    history.subscription.customer.metadata.userid AS mozilla_account_id,
    history.subscription.customer.metadata.userid_sha256 AS mozilla_account_id_sha256,
    CASE
      -- Use the same address hierarchy as Stripe Tax after we enabled Stripe Tax (FXA-5457).
      -- https://stripe.com/docs/tax/customer-locations#address-hierarchy
      WHEN DATE(history.valid_to) >= '2022-12-01'
        AND (
          DATE(history.subscription.ended_at) >= '2022-12-01'
          OR history.subscription.ended_at IS NULL
        )
        THEN COALESCE(
            NULLIF(history.subscription.customer.shipping.address.country, ''),
            NULLIF(history.subscription.customer.address.country, ''),
            charge_summaries.latest_card_country
          )
      -- SubPlat copies the PayPal billing agreement country to the customer's address.
      WHEN paypal_subscriptions.subscription_id IS NOT NULL
        THEN NULLIF(history.subscription.customer.address.country, '')
      ELSE charge_summaries.latest_card_country
    END AS country_code,
    plan_services.services,
    subscription_item.plan.product.id AS provider_product_id,
    subscription_item.plan.product.name AS product_name,
    subscription_item.plan.id AS provider_plan_id,
    subscription_item.plan.`interval` AS plan_interval_type,
    subscription_item.plan.interval_count AS plan_interval_count,
    UPPER(subscription_item.plan.currency) AS plan_currency,
    (CAST(subscription_item.plan.amount AS DECIMAL) / 100) AS plan_amount,
    IF(ARRAY_LENGTH(plan_services.services) > 1, TRUE, FALSE) AS is_bundle,
    IF(
      history.subscription.status = 'trialing'
      OR (
        history.subscription.ended_at
        BETWEEN history.subscription.trial_start
        AND history.subscription.trial_end
      ),
      TRUE,
      FALSE
    ) AS is_trial,
    history.subscription_is_active AS is_active,
    history.subscription.status AS provider_status,
    history.subscription_first_active_at AS started_at,
    history.subscription.ended_at,
    -- TODO: ended_reason
    IF(
      history.subscription.ended_at IS NULL,
      history.subscription.current_period_start,
      NULL
    ) AS current_period_started_at,
    IF(
      history.subscription.ended_at IS NULL,
      history.subscription.current_period_end,
      NULL
    ) AS current_period_ends_at,
    history.subscription.cancel_at_period_end IS NOT TRUE AS auto_renew,
    IF(
      history.subscription.cancel_at_period_end,
      history.subscription.canceled_at,
      NULL
    ) AS auto_renew_disabled_at,
    initial_discounts.coupon_name AS initial_discount_name,
    initial_discounts.promotion_code AS initial_discount_promotion_code,
    current_period_discounts.coupon_name AS current_period_discount_name,
    current_period_discounts.promotion_code AS current_period_discount_promotion_code,
    calculate_discount_amount(
      current_period_discounts.amount_off,
      current_period_discounts.percent_off,
      subscription_item.plan.amount
    ) AS current_period_discount_amount,
    ongoing_discounts.coupon_name AS ongoing_discount_name,
    ongoing_discounts.promotion_code AS ongoing_discount_promotion_code,
    calculate_discount_amount(
      ongoing_discounts.amount_off,
      ongoing_discounts.percent_off,
      subscription_item.plan.amount
    ) AS ongoing_discount_amount,
    ongoing_discounts.ends_at AS ongoing_discount_ends_at,
    COALESCE(charge_summaries.has_refunds, FALSE) AS has_refunds,
    COALESCE(charge_summaries.has_fraudulent_charges, FALSE) AS has_fraudulent_charges
  ) AS subscription
FROM
  active_subscriptions_history AS history
CROSS JOIN
  UNNEST(history.subscription.items) AS subscription_item
LEFT JOIN
  plan_services
  ON subscription_item.plan.id = plan_services.plan_id
LEFT JOIN
  paypal_subscriptions
  ON history.subscription.id = paypal_subscriptions.subscription_id
LEFT JOIN
  subscriptions_history_charge_summaries AS charge_summaries
  ON history.id = charge_summaries.subscriptions_history_id
LEFT JOIN
  subscription_initial_discounts AS initial_discounts
  ON history.subscription.id = initial_discounts.subscription_id
LEFT JOIN
  subscription_period_discounts AS current_period_discounts
  ON history.subscription.id = current_period_discounts.subscription_id
  AND history.subscription.current_period_start = current_period_discounts.period_start
  AND history.subscription.current_period_end = current_period_discounts.period_end
LEFT JOIN
  subscriptions_history_ongoing_discounts AS ongoing_discounts
  ON history.id = ongoing_discounts.subscriptions_history_id
