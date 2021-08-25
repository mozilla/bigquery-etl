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
    COALESCE(ended_at, TIMESTAMP(CURRENT_DATE)) AS end_date,
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
    attribution.entrypoint_experiment,
    attribution.entrypoint_variation,
    attribution.utm_campaign,
    attribution.utm_content,
    attribution.utm_medium,
    attribution.utm_source,
    attribution.utm_term,
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
    user_id,
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
    end_date,
    fxa_uid,
    country,
    country_name,
    user_registration_date,
    entrypoint_experiment,
    entrypoint_variation,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term,
    CONCAT("FxA ", provider) AS provider,
    plan_amount,
    billing_scheme,
    plan_currency,
    plan_interval,
    plan_interval_count,
    "Etc/UTC" AS plan_interval_timezone,
    product_id,
    product_name,
    CONCAT(
      plan_interval_count,
      "-",
      plan_interval,
      "-",
      plan_currency,
      "-",
      (plan_amount / 100)
    ) AS pricing_plan,
    -- Stripe default billing grace period is 1 day and Paypal is billed by Stripe
    INTERVAL 1 DAY AS billing_grace_period,
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
    user_id,
    CAST(user_id AS STRING) AS customer_id,
    CAST(id AS STRING) AS subscription_id,
    CAST(NULL AS STRING) AS plan_id,
    CAST(NULL AS STRING) AS status,
    updated_at AS event_timestamp,
    TIMESTAMP(
      MIN(start_time) OVER (
        PARTITION BY
          user_id
        ROWS BETWEEN
          UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      )
    ) AS customer_start_date,
    start_time AS subscription_start_date,
    created_at AS created,
    CAST(NULL AS TIMESTAMP) AS trial_end,
    CAST(NULL AS TIMESTAMP) AS canceled_at,
    CAST(NULL AS STRING) AS canceled_for_customer_at,
    CAST(NULL AS TIMESTAMP) AS cancel_at,
    CAST(NULL AS BOOL) AS cancel_at_period_end,
    IF(end_time < TIMESTAMP(CURRENT_DATE), end_time, NULL) AS ended_at,
    LEAST(end_time, TIMESTAMP(CURRENT_DATE)) AS end_date,
    fxa_uid,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS country_name,
    user_registration_date,
    entrypoint_experiment,
    entrypoint_variation,
    utm_campaign,
    utm_content,
    utm_medium,
    utm_source,
    utm_term,
    "Apple Store IAP" AS provider,
    NULL AS plan_amount,
    CAST(NULL AS STRING) AS billing_scheme,
    CAST(NULL AS STRING) AS plan_currency,
    `interval` AS plan_interval,
    interval_count AS plan_interval_count,
    "America/Los_Angeles" AS plan_interval_timezone,
    CAST(NULL AS STRING) AS product_id,
    "Mozilla VPN" AS product_name,
    CONCAT(interval_count, "-", `interval`, "-", "apple") AS pricing_plan,
    -- Apple bills recurring subscriptions before they end
    INTERVAL 0 DAY AS billing_grace_period,
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
  mozfun.norm.vpn_attribution(
    utm_campaign => utm_campaign,
    utm_content => utm_content,
    utm_medium => utm_medium,
    utm_source => utm_source
  ).*,
  mozfun.norm.diff_months(
    start => DATETIME(subscription_start_date, plan_interval_timezone),
    `end` => DATETIME(end_date, plan_interval_timezone),
    grace_period => billing_grace_period,
    inclusive => FALSE
  ) AS months_retained,
  mozfun.norm.diff_months(
    start => DATETIME(subscription_start_date, plan_interval_timezone),
    `end` => DATETIME(TIMESTAMP(CURRENT_DATE), plan_interval_timezone),
    grace_period => billing_grace_period,
    inclusive => FALSE
  ) AS current_months_since_subscription_start,
FROM
  vpn_subscriptions
WHERE
  -- exclude subscriptions that never left the trial period
  DATE(subscription_start_date) < DATE(end_date)
