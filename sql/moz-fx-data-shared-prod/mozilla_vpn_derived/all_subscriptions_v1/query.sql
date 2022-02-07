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
standardized_country AS (
  SELECT
    raw_country AS country,
    standardized_country AS country_name,
  FROM
    mozdata.static.third_party_standardized_country_names
),
attribution AS (
  SELECT
    fxa_uid,
    ARRAY_AGG(attribution ORDER BY attribution.timestamp LIMIT 1)[OFFSET(0)].*,
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.fxa_attribution_v1
  CROSS JOIN
    UNNEST(fxa_uids) AS fxa_uid
  WHERE
    attribution IS NOT NULL
  GROUP BY
    fxa_uid
),
users AS (
  SELECT
    id AS user_id,
    fxa_uid,
    created_at AS user_registration_date,
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
    provider,
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
    IF(promotion_code IS NULL, [], [promotion_code]) AS promotion_codes,
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
    users
  USING
    (fxa_uid)
  LEFT JOIN
    attribution
  USING
    (fxa_uid)
  LEFT JOIN
    stripe_promotion_codes
  USING
    (promotion_code_id)
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
    "Apple Store" AS provider,
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
    CAST(NULL AS ARRAY<STRING>) AS promotion_codes,
  FROM
    mozdata.mozilla_vpn.subscriptions
  CROSS JOIN
    UNNEST(apple_receipt.active_periods)
  LEFT JOIN
    users
  USING
    (user_id)
  LEFT JOIN
    attribution
  USING
    (fxa_uid)
  WHERE
    apple_receipt.environment = "Production"
),
android_iap_events AS (
  SELECT
    document_id,
    NULLIF(`timestamp`, "1970-01-01 00:00:00") AS event_timestamp,
    mozfun.iap.parse_android_receipt(`data`).*
  FROM
    `moz-fx-fxa-prod-0712.firestore_export.iap_google_raw_changelog`
),
android_iap_aggregates AS (
  SELECT
    document_id,
    MIN(event_timestamp) AS created,
    ANY_VALUE(user_id) AS fxa_uid,
    ARRAY_CONCAT(
      ARRAY_CONCAT_AGG(IF(event_timestamp >= expiry_time, [expiry_time], [])),
      [MAX(expiry_time)]
    ) AS end_times,
    ARRAY_CONCAT(
      [MIN(start_time)],
      ARRAY_CONCAT_AGG(IF(event_timestamp < expiry_time, [event_timestamp], []))
    ) AS start_times,
    ARRAY_AGG(
      STRUCT(
        auto_renewing,
        country_code,
        package_name,
        payment_state,
        price_amount_micros,
        price_currency_code,
        sku,
        user_cancellation_time,
        event_timestamp
      )
      ORDER BY
        event_timestamp DESC
      LIMIT
        1
    )[OFFSET(0)].*,
  FROM
    android_iap_events
  WHERE
    form_of_payment = "GOOGLE_PLAY"
    AND package_name = "org.mozilla.firefox.vpn"
  GROUP BY
    document_id
),
android_iap_periods AS (
  SELECT
    IF(
      active_dates_offset > 0,
      document_id || "-" || active_dates_offset,
      document_id
    ) AS subscription_id,
    fxa_uid,
    created,
    event_timestamp,
    start_time,
    end_time,
    DIV(price_amount_micros, 10000) AS plan_amount,
    LOWER(price_currency_code) AS plan_currency,
    STRING(user_cancellation_time) AS canceled_for_customer_at,
    package_name AS product_id,
    sku AS plan_id,
    LOWER(country_code) AS country,
    (
      CASE
      WHEN
        ENDS_WITH(sku, ".1_month_subscription")
        OR ENDS_WITH(sku, ".monthly")
      THEN
        STRUCT("month" AS plan_interval, 1 AS plan_interval_count)
      WHEN
        ENDS_WITH(sku, ".6_month_subscription")
      THEN
        ("month", 6)
      WHEN
        ENDS_WITH(sku, ".12_month_subscription")
      THEN
        ("year", 1)
      WHEN
        ENDS_WITH(sku, ".1_day_subscription")
      THEN
        -- only used for testing
        ("day", 1)
      END
    ).*,
    (
      auto_renewing
      AND payment_state = 0
      -- only the last set of active dates can be in the billing grace period
      AND 1 = ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY active_dates_offset DESC)
    ) AS in_billing_grace_period,
  FROM
    android_iap_aggregates
  LEFT JOIN
    UNNEST(
      ARRAY(
        SELECT AS STRUCT
          (
            SELECT
              MIN(end_time)
            FROM
              UNNEST(end_times) AS end_time
            WHERE
              end_time > start_time
          ) AS end_time,
          MIN(start_time) AS start_time,
        FROM
          UNNEST(start_times) AS start_time
        GROUP BY
          end_time
        ORDER BY
          start_time
      )
    )
    WITH OFFSET AS active_dates_offset
),
android_iap_subscriptions AS (
  SELECT
    user_id,
    fxa_uid AS customer_id,
    subscription_id,
    plan_id,
    CAST(NULL AS STRING) AS status,
    event_timestamp,
    TIMESTAMP(
      MIN(start_time) OVER (
        PARTITION BY
          fxa_uid
        ROWS BETWEEN
          UNBOUNDED PRECEDING
          AND UNBOUNDED FOLLOWING
      )
    ) AS customer_start_date,
    start_time AS subscription_start_date,
    created,
    CAST(NULL AS TIMESTAMP) AS trial_end,
    CAST(NULL AS TIMESTAMP) AS canceled_at,
    canceled_for_customer_at,
    CAST(NULL AS TIMESTAMP) AS cancel_at,
    CAST(NULL AS BOOL) AS cancel_at_period_end,
    IF(
      in_billing_grace_period,
      -- android subscriptions in grace period have not ended
      STRUCT(CAST(NULL AS TIMESTAMP) AS ended_at, TIMESTAMP(CURRENT_DATE) AS end_date),
      STRUCT(
        IF(end_time < TIMESTAMP(CURRENT_DATE), end_time, NULL) AS ended_at,
        LEAST(end_time, TIMESTAMP(CURRENT_DATE)) AS end_date
      )
    ).*,
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
    "Google Play" AS provider,
    plan_amount,
    CAST(NULL AS STRING) AS billing_scheme,
    plan_currency,
    plan_interval,
    plan_interval_count,
    "Etc/UTC" AS plan_interval_timezone,
    product_id,
    "Mozilla VPN" AS product_name,
    CONCAT(
      plan_interval_count,
      "-",
      plan_interval,
      "-",
      plan_currency,
      "-",
      (plan_amount / 100)
    ) AS pricing_plan,
    IF(in_billing_grace_period, INTERVAL 1 MONTH, INTERVAL 0 DAY) AS billing_grace_period,
    CAST(NULL AS ARRAY<STRING>) AS promotion_codes,
  FROM
    android_iap_periods
  LEFT JOIN
    standardized_country
  USING
    (country)
  LEFT JOIN
    users
  USING
    (fxa_uid)
  LEFT JOIN
    attribution
  USING
    (fxa_uid)
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
  UNION ALL
  SELECT
    *
  FROM
    android_iap_subscriptions
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
