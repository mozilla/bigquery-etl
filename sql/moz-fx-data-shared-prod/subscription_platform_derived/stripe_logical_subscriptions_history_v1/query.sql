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
    COALESCE(
      NULLIF(history.customer.shipping.address.country, ''),
      NULLIF(history.customer.address.country, '')
      -- TODO: also use charge billing addresses
    ) AS country_code,
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
-- Exclude subscriptions which have never been active.
QUALIFY
  LOGICAL_OR(subscription.is_active) OVER (PARTITION BY subscription.id)
