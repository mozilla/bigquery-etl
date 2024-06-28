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
      IF(
        cards.country IS NOT NULL,
        STRUCT(
          cards.country AS latest_card_country,
          COALESCE(
            charge_us_zip.state_code,
            charge_ca_post.province_code
          ) AS latest_card_state
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        -- Prefer charges that succeeded
        IF(charges.status = 'succeeded', 1, 2),
        charges.created DESC
    )[SAFE_ORDINAL(1)].*,
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
  -- charges usually have postal code and are sometimes associated with
  -- a card that does not have a state or postal code
  LEFT JOIN
    `moz-fx-data-shared-prod.static.us_zip_code_prefixes_v1` AS charge_us_zip
    ON cards.country = "US"
    AND LEFT(charges.billing_detail_address_postal_code, 3) = charge_us_zip.zip_code_prefix
  LEFT JOIN
    `moz-fx-data-shared-prod.static.ca_postal_districts_v1` AS charge_ca_post
    ON cards.country = "CA"
    AND UPPER(
      LEFT(charges.billing_detail_address_postal_code, 1)
    ) = charge_ca_post.postal_district_code
  GROUP BY
    subscriptions_history_id
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
  (
    SELECT AS STRUCT
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
      (
        CASE
          -- Use the same address hierarchy as Stripe Tax after we enabled Stripe Tax on 2022-12-01 (FXA-5457).
          -- https://stripe.com/docs/tax/customer-locations#address-hierarchy
          WHEN DATE(history.valid_to) >= '2022-12-01'
            AND (
              DATE(history.subscription.ended_at) >= '2022-12-01'
              OR history.subscription.ended_at IS NULL
            )
            THEN
              CASE
                WHEN history.subscription.customer.shipping.address.country IS NOT NULL
                  THEN STRUCT(
                      history.subscription.customer.shipping.address.country AS country_code,
                      history.subscription.customer.shipping.address.state AS state_code
                    )
                WHEN history.subscription.customer.address.country IS NOT NULL
                  THEN STRUCT(
                      history.subscription.customer.address.country AS country_code,
                      history.subscription.customer.address.state AS state_code
                    )
                ELSE STRUCT(
                    charge_summaries.latest_card_country AS country_code,
                    charge_summaries.latest_card_state AS state_code
                  )
              END
          -- SubPlat copied the PayPal billing agreement country to the customer's address before we enabled Stripe Tax (FXA-5457).
          WHEN paypal_subscriptions.subscription_id IS NOT NULL
            THEN STRUCT(
                history.subscription.customer.address.country AS country_code,
                history.subscription.customer.address.state AS state_code
              )
          ELSE STRUCT(
              charge_summaries.latest_card_country AS country_code,
              charge_summaries.latest_card_state AS state_code
            )
        END
      ).*,
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
      -- TODO: promotion_codes
      -- TODO: promotion_discounts_amount
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
