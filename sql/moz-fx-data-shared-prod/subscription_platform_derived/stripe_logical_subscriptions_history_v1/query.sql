WITH plan_services AS (
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
  ON
    plan_id IN UNNEST(tier.stripe_plan_ids)
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
subscription_charges AS (
  SELECT
    invoices.subscription_id,
    charges.created,
    charges.status,
    cards.country AS card_country
  FROM
    `moz-fx-data-shared-prod.stripe_external.charge_v1` AS charges
  JOIN
    `moz-fx-data-shared-prod.stripe_external.invoice_v1` AS invoices
  ON
    charges.invoice_id = invoices.id
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.card_v1` AS cards
  ON
    charges.card_id = cards.id
),
subscriptions_history_latest_card_countries AS (
  SELECT
    history.id AS subscriptions_history_id,
    ARRAY_AGG(
      subscription_charges.card_country IGNORE NULLS
      ORDER BY
        -- Prefer charges that succeeded.
        IF(subscription_charges.status = 'succeeded', 1, 2),
        subscription_charges.created DESC
      LIMIT
        1
    )[SAFE_ORDINAL(1)] AS card_country
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_history_v2` AS history
  JOIN
    subscription_charges
  ON
    history.subscription.id = subscription_charges.subscription_id
    AND subscription_charges.created < history.valid_to
  GROUP BY
    subscriptions_history_id
)
SELECT
  CONCAT(
    'Stripe-',
    history.subscription.id,
    '-',
    subscription_item.id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', history.subscription.start_date),
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
      subscription_item.id,
      '-',
      FORMAT_TIMESTAMP('%FT%H:%M:%E6S', history.subscription.start_date)
    ) AS id,
    'Stripe' AS provider,
    IF(paypal_subscriptions.subscription_id IS NOT NULL, 'PayPal', 'Stripe') AS payment_provider,
    history.subscription.id AS provider_subscription_id,
    subscription_item.id AS provider_subscription_item_id,
    history.subscription.created AS provider_subscription_created_at,
    history.subscription.customer_id AS provider_customer_id,
    history.customer.metadata.userid AS mozilla_account_id,
    history.customer.metadata.userid_sha256 AS mozilla_account_id_sha256,
    CASE
      -- Use the same address hierarchy as Stripe Tax after we enabled Stripe Tax (FXA-5457).
      -- https://stripe.com/docs/tax/customer-locations#address-hierarchy
      WHEN DATE(history.valid_to) >= '2022-12-01'
        AND (
          DATE(history.subscription.ended_at) >= '2022-12-01'
          OR history.subscription.ended_at IS NULL
        )
        THEN COALESCE(
            NULLIF(history.customer.shipping.address.country, ''),
            NULLIF(history.customer.address.country, ''),
            latest_card_countries.card_country
          )
      -- SubPlat copies the PayPal billing agreement country to the customer's address.
      WHEN paypal_subscriptions.subscription_id IS NOT NULL
        THEN NULLIF(history.customer.address.country, '')
      ELSE latest_card_countries.card_country
    END AS country_code,
    plan_services.services,
    subscription_item.plan.product.id AS provider_product_id,
    subscription_item.plan.product.name AS product_name,
    subscription_item.plan.id AS provider_plan_id,
    subscription_item.plan.`interval` AS plan_interval,
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
    -- The `is_active` logic should agree with what SubPlat considers an active Stripe subscription.
    -- https://github.com/mozilla/fxa/blob/56026cd08e60525823c60c4f4116f705e79d6124/packages/fxa-shared/subscriptions/stripe.ts#L19-L24
    history.subscription.status IN ('active', 'past_due', 'trialing') AS is_active,
    history.subscription.status AS provider_status,
    history.subscription.start_date AS started_at,
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
    ) AS auto_renew_disabled_at
    -- TODO: promotion_codes
    -- TODO: promotion_discounts_amount
    -- TODO: has_fraudulent_charges
  ) AS subscription
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_history_v2` AS history
CROSS JOIN
  UNNEST(history.subscription.items) AS subscription_item
LEFT JOIN
  plan_services
ON
  subscription_item.plan.id = plan_services.plan_id
LEFT JOIN
  paypal_subscriptions
ON
  history.subscription.id = paypal_subscriptions.subscription_id
LEFT JOIN
  subscriptions_history_latest_card_countries AS latest_card_countries
ON
  history.id = latest_card_countries.subscriptions_history_id
-- Exclude subscriptions which have never been active.
QUALIFY
  LOGICAL_OR(subscription.is_active) OVER (PARTITION BY subscription.id)
