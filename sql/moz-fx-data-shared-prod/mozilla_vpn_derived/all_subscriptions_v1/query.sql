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
    cancel_at,
    cancel_at_period_end,
    ended_at,
    COALESCE(ended_at, CURRENT_TIMESTAMP) AS end_date,
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
paypal_country AS (
  SELECT
    fxa_uid,
    plan_id,
    ARRAY_AGG(source_country ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] AS country,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.fxa_pay_setup_complete_v1
  WHERE
    LOWER(payment_provider) = "paypal"
  GROUP BY
    fxa_uid,
    plan_id
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
      -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1712027#c1
      -- for why presence of "paypalTransactionId" alone is insufficient
      "paypalTransactionId" IN (SELECT key FROM UNNEST(invoices_v1.metadata))
      OR (invoices_v1.charge IS NULL AND invoices_v1.status = "paid"),
      STRUCT("Paypal" AS provider, paypal_country.country),
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
  LEFT JOIN
    paypal_country
  ON
    stripe_customers.fxa_uid = paypal_country.fxa_uid
    AND lines.plan.id = paypal_country.plan_id
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
standardized_country AS (
  SELECT
    raw_country AS country,
    standardized_country AS country_name,
  FROM
    mozdata.static.third_party_standardized_country_names
),
attribution AS (
  SELECT
    id AS user_id,
    fxa_uid,
    created_at AS user_registration_date,
    mozfun.map.get_key(attribution, "referrer") AS referrer,
    mozfun.map.get_key(attribution, "utm_medium") AS utm_medium,
    mozfun.map.get_key(attribution, "utm_source") AS utm_source,
    mozfun.map.get_key(attribution, "utm_content") AS utm_content,
    mozfun.map.get_key(attribution, "utm_campaign") AS utm_campaign,
    attribution_category,
    coarse_attribution_category,
  FROM
    mozdata.mozilla_vpn.users
),
stripe_vpn_plans AS (
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
  WHERE
    products.name = "Mozilla VPN"
),
fxa_subscriptions AS (
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
    cancel_at,
    cancel_at_period_end,
    ended_at,
    end_date,
    fxa_uid,
    country,
    country_name,
    user_registration_date,
    referrer,
    utm_medium,
    utm_source,
    utm_content,
    utm_campaign,
    attribution_category,
    coarse_attribution_category,
    CONCAT("FxA ", provider) AS provider,
    plan_amount,
    billing_scheme,
    plan_currency,
    plan_interval,
    product_id,
    product_name,
  FROM
    stripe_subscriptions
  JOIN -- exclude subscriptions to non-vpn products
    stripe_vpn_plans
  USING
    (plan_id)
  LEFT JOIN
    stripe_subscription_provider_country
  USING
    (subscription_id)
  LEFT JOIN
    standardized_country
  USING
    (country)
  LEFT JOIN
    stripe_customers
  USING
    (customer_id)
  LEFT JOIN
    attribution
  USING
    (fxa_uid)
),
apple_iap_subscriptions AS (
  SELECT
    CAST(user_id AS STRING) AS customer_id,
    CAST(id AS STRING) AS subscription_id,
    CAST(NULL AS STRING) AS plan_id,
    CAST(NULL AS STRING) AS status,
    updated_at AS event_timestamp,
    TIMESTAMP(
      MIN(start_date) OVER (
        PARTITION BY
          user_id
        ROWS BETWEEN
          UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      )
    ) AS customer_start_date,
    TIMESTAMP(start_date) AS subscription_start_date,
    created_at AS created,
    CAST(NULL AS TIMESTAMP) AS trial_end,
    CAST(NULL AS TIMESTAMP) AS canceled_at,
    CAST(NULL AS TIMESTAMP) AS cancel_at,
    CAST(NULL AS BOOL) AS cancel_at_period_end,
    IF(end_date < CURRENT_DATE, TIMESTAMP(end_date), NULL) AS ended_at,
    TIMESTAMP(LEAST(end_date, CURRENT_DATE)) AS end_date,
    fxa_uid,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS country_name,
    user_registration_date,
    referrer,
    utm_medium,
    utm_source,
    utm_content,
    utm_campaign,
    attribution_category,
    coarse_attribution_category,
    "Apple Store IAP" AS provider,
    IF(`interval` = "month" AND interval_count = 1, 499, NULL) AS plan_amount,
    CAST(NULL AS STRING) AS billing_scheme,
    IF(`interval` = "month" AND interval_count = 1, "USD", NULL) AS plan_currency,
    `interval` AS plan_interval,
    interval_count AS plan_interval_count,
    CAST(NULL AS STRING) AS product_id,
    "Mozilla VPN" AS product_name,
  FROM
    mozdata.mozilla_vpn.subscriptions
  CROSS JOIN
    UNNEST(apple_receipt.active_periods)
  LEFT JOIN
    attribution
  USING
    (user_id)
  WHERE
    apple_receipt.environment = "Production"
),
vpn_subscriptions AS (
  SELECT
    *
  FROM
    fxa_subscriptions
  UNION ALL
  SELECT
    *
  FROM
    apple_iap_subscriptions
)
SELECT
  *,
  CONCAT(
    plan_interval_count,
    "-",
    plan_interval,
    "-",
    plan_currency,
    "-",
    (plan_amount / 100)
  ) AS pricing_plan,
  mozfun.norm.vpn_attribution(
    provider,
    referrer,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source
  ).*,
FROM
  vpn_subscriptions
WHERE
  -- exclude subscriptions that never left the trial period
  DATE(subscription_start_date) < DATE(end_date)
