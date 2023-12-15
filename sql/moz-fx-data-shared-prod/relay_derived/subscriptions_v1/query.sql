WITH standardized_country AS (
  SELECT
    raw_country AS country,
    standardized_country AS country_name,
  FROM
    `moz-fx-data-shared-prod`.static.third_party_standardized_country_names
),
stripe_subscriptions_history AS (
  SELECT
    *,
    CONCAT(
      subscription_id,
      COALESCE(
        CONCAT(
          "-",
          NULLIF(ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY valid_from), 1)
        ),
        ""
      )
    ) AS subscription_sequence_id
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_subscriptions_history_v1`
  WHERE
    -- Only include the current history records and the last history records for previous plans.
    (valid_to IS NULL OR plan_ended_at IS NOT NULL)
    AND status NOT IN ("incomplete", "incomplete_expired")
),
relay_subscriptions AS (
  SELECT
    -- user_id,
    customer_id,
    subscription_sequence_id AS subscription_id,
    IF(
      subscription_sequence_id != subscription_id,
      subscription_id,
      NULL
    ) AS original_subscription_id,
    plan_id,
    status,
    synced_at AS event_timestamp,
    IF(
      (trial_end > TIMESTAMP(CURRENT_DATE) OR ended_at <= trial_end),
      NULL,
      COALESCE(plan_started_at, subscription_start_date)
    ) AS subscription_start_date,
    --first subscription start date associated with the subscription id
    IF(
      (trial_end > TIMESTAMP(CURRENT_DATE) OR ended_at <= trial_end),
      NULL,
      subscription_start_date
    ) AS original_subscription_start_date,
    IF(plan_started_at IS NOT NULL, "Plan Change", NULL) AS subscription_start_reason,
    -- created,
    trial_start,
    trial_end,
    canceled_at,
    canceled_for_customer_at,
    cancel_at,
    cancel_at_period_end,
    COALESCE(plan_ended_at, IF(ended_at < TIMESTAMP(CURRENT_DATE), ended_at, NULL)) AS ended_at,
    IF(plan_ended_at IS NOT NULL, "Plan Change", NULL) AS ended_reason,
    fxa_uid,
    country,
    country_name,
    -- user_registration_date,
    -- entrypoint_experiment,
    -- entrypoint_variation,
    -- utm_campaign,
    -- utm_content,
    -- utm_medium,
    -- utm_source,
    -- utm_term,
    provider,
    plan_amount,
    -- billing_scheme,
    plan_currency,
    plan_interval,
    plan_interval_count,
    plan_interval_timezone,
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
    -- Stripe billing grace period is 7 day and Paypal is billed by Stripe
    INTERVAL 7 DAY AS billing_grace_period,
    has_refunds,
    has_fraudulent_charges,
    has_fraudulent_charge_refunds,
    promotion_codes,
    promotion_discounts_amount,
  FROM
    stripe_subscriptions_history
  LEFT JOIN
    standardized_country
  USING
    (country)
  -- LEFT JOIN
  --   users
  -- USING
  --   (fxa_uid)
  -- LEFT JOIN
  --   attribution
  -- USING
  --   (fxa_uid)
  WHERE
    (
      "premium-relay" IN UNNEST(stripe_subscriptions_history.product_capabilities)
      OR "relay-phones" IN UNNEST(stripe_subscriptions_history.product_capabilities)
      OR "premium-relay" IN UNNEST(stripe_subscriptions_history.plan_capabilities)
      OR "relay-phones" IN UNNEST(stripe_subscriptions_history.plan_capabilities)
    )
),
relay_subscriptions_with_end_date AS (
  SELECT
    *,
    IF(
      customer_id IS NOT NULL,
      MIN(subscription_start_date) OVER (PARTITION BY customer_id),
      subscription_start_date
    ) AS customer_start_date,
    COALESCE(ended_at, TIMESTAMP(CURRENT_DATE)) AS end_date,
  FROM
    relay_subscriptions
)
SELECT
  * REPLACE (
    CASE
      WHEN subscription_start_date IS NULL
        THEN NULL
      WHEN subscription_start_reason IS NOT NULL
        THEN subscription_start_reason
      WHEN trial_start IS NOT NULL
        THEN "Converted Trial"
      WHEN DATE(subscription_start_date) = DATE(customer_start_date)
        THEN "New"
      ELSE "Resurrected"
    END AS subscription_start_reason,
    CASE
      WHEN ended_at IS NULL
        THEN NULL
      WHEN ended_reason IS NOT NULL
        THEN ended_reason
      WHEN canceled_for_customer_at IS NOT NULL
        OR cancel_at_period_end
        THEN "Cancelled by Customer"
      ELSE "Payment Failed"
    END AS ended_reason
  ),
  mozfun.norm.diff_months(
    start => DATETIME(subscription_start_date, plan_interval_timezone),
    `end` => DATETIME(end_date, plan_interval_timezone),
    grace_period => billing_grace_period,
    inclusive => FALSE
  ) AS months_retained,
  mozfun.norm.diff_months(
    start => DATETIME(
      COALESCE(original_subscription_start_date, subscription_start_date),
      plan_interval_timezone
    ),
    `end` => DATETIME(end_date, plan_interval_timezone),
    grace_period => billing_grace_period,
    inclusive => FALSE
  ) AS original_subscription_months_retained,
  mozfun.norm.diff_months(
    start => DATETIME(subscription_start_date, plan_interval_timezone),
    `end` => DATETIME(TIMESTAMP(CURRENT_DATE), plan_interval_timezone),
    grace_period => billing_grace_period,
    inclusive => FALSE
  ) AS current_months_since_subscription_start,
  mozfun.norm.diff_months(
    start => DATETIME(
      COALESCE(original_subscription_start_date, subscription_start_date),
      plan_interval_timezone
    ),
    `end` => DATETIME(TIMESTAMP(CURRENT_DATE), plan_interval_timezone),
    grace_period => billing_grace_period,
    inclusive => FALSE
  ) AS current_months_since_original_subscription_start,
FROM
  relay_subscriptions_with_end_date
