WITH original_subscriptions_history AS (
  SELECT
    id,
    `timestamp` AS valid_from,
    COALESCE(
      LEAD(`timestamp`) OVER (PARTITION BY subscription.id ORDER BY `timestamp`),
      '9999-12-31 23:59:59.999999'
    ) AS valid_to,
    id AS stripe_subscriptions_revised_changelog_id,
    subscription
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_subscriptions_revised_changelog_v1
),
subscriptions_history AS (
  -- Include customers as they existed at the start of each subscription history period.
  SELECT
    subscriptions_history.id,
    subscriptions_history.valid_from,
    IF(
      customers_history.valid_to IS NOT NULL,
      LEAST(subscriptions_history.valid_to, customers_history.valid_to),
      subscriptions_history.valid_to
    ) AS valid_to,
    subscriptions_history.stripe_subscriptions_revised_changelog_id,
    customers_history.stripe_customers_revised_changelog_id,
    subscriptions_history.subscription,
    customers_history.customer
  FROM
    original_subscriptions_history AS subscriptions_history
  LEFT JOIN
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_customers_history_v1 AS customers_history
  ON
    subscriptions_history.subscription.customer_id = customers_history.customer.id
    AND subscriptions_history.valid_from >= customers_history.valid_from
    AND subscriptions_history.valid_from < customers_history.valid_to
  UNION ALL
  -- Include customer changes during the subscription history periods.
  SELECT
    CONCAT(
      subscriptions_history.subscription.id,
      '-',
      FORMAT_TIMESTAMP('%FT%H:%M:%E6S', customers_history.valid_from)
    ) AS id,
    customers_history.valid_from,
    LEAST(subscriptions_history.valid_to, customers_history.valid_to) AS valid_to,
    subscriptions_history.stripe_subscriptions_revised_changelog_id,
    customers_history.stripe_customers_revised_changelog_id,
    subscriptions_history.subscription,
    customers_history.customer
  FROM
    original_subscriptions_history AS subscriptions_history
  JOIN
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_customers_history_v1 AS customers_history
  ON
    subscriptions_history.subscription.customer_id = customers_history.customer.id
    AND subscriptions_history.valid_from < customers_history.valid_from
    AND subscriptions_history.valid_to > customers_history.valid_from
),
subscriptions_history_invoice_summaries AS (
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
    LOGICAL_OR(JSON_VALUE(invoices.metadata, '$.paypalTransactionId') IS NOT NULL) AS uses_paypal,
    LOGICAL_OR(
      refunds.status = 'succeeded'
      OR JSON_VALUE(invoices.metadata, '$.paypalRefundTransactionId') IS NOT NULL
    ) AS has_refunds,
    LOGICAL_OR(
      charges.fraud_details_user_report = 'fraudulent'
      OR (
        charges.fraud_details_stripe_report = 'fraudulent'
        AND charges.fraud_details_user_report IS DISTINCT FROM 'safe'
      )
      OR (refunds.reason = 'fraudulent' AND refunds.status = 'succeeded')
    ) AS has_fraudulent_charges
  FROM
    `moz-fx-data-shared-prod.stripe_external.invoice_v1` AS invoices
  JOIN
    subscriptions_history AS history
  ON
    invoices.subscription_id = history.subscription.id
    AND invoices.created < history.valid_to
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.charge_v1` AS charges
  ON
    invoices.id = charges.invoice_id
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.card_v1` AS cards
  ON
    charges.card_id = cards.id
  LEFT JOIN
    `moz-fx-data-shared-prod.stripe_external.refund_v1` AS refunds
  ON
    charges.id = refunds.charge_id
  GROUP BY
    subscriptions_history_id
)
SELECT
  history.id,
  history.valid_from,
  history.valid_to,
  history.stripe_subscriptions_revised_changelog_id,
  history.stripe_customers_revised_changelog_id,
  (
    SELECT AS STRUCT
      history.subscription.*,
      IF(invoice_summaries.uses_paypal, 'PayPal', 'Stripe') AS payment_provider,
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
              invoice_summaries.latest_card_country
            )
        -- SubPlat copies the PayPal billing agreement country to the customer's address.
        WHEN invoice_summaries.uses_paypal
          THEN NULLIF(history.customer.address.country, '')
        ELSE invoice_summaries.latest_card_country
      END AS country_code,
      COALESCE(invoice_summaries.has_refunds, FALSE) AS has_refunds,
      COALESCE(invoice_summaries.has_fraudulent_charges, FALSE) AS has_fraudulent_charges
  ) AS subscription,
  history.customer
FROM
  subscriptions_history AS history
LEFT JOIN
  subscriptions_history_invoice_summaries AS invoice_summaries
ON
  history.id = invoice_summaries.subscriptions_history_id
