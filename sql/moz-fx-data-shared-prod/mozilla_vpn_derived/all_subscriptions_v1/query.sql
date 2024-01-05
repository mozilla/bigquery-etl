WITH standardized_country AS (
  SELECT
    raw_country AS country,
    standardized_country AS country_name,
  FROM
    `moz-fx-data-shared-prod`.static.third_party_standardized_country_names
),
fxa_attributions AS (
  SELECT
    fxa_uid,
    attribution
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.fxa_attribution_v1
  CROSS JOIN
    UNNEST(fxa_uids) AS fxa_uid
  WHERE
    attribution IS NOT NULL
),
users AS (
  SELECT
    id AS user_id,
    fxa_uid,
    created_at AS user_registration_date,
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn.users
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
    `moz-fx-data-shared-prod`.subscription_platform_derived.stripe_subscriptions_history_v1
  WHERE
    -- Only include the current history records and the last history records for previous plans.
    (valid_to IS NULL OR plan_ended_at IS NOT NULL)
    AND status NOT IN ("incomplete", "incomplete_expired")
),
stripe_subscriptions AS (
  SELECT
    user_id,
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
    created,
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
    state,
    user_registration_date,
    provider,
    plan_amount,
    billing_scheme,
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
  LEFT JOIN
    users
  USING
    (fxa_uid)
  WHERE
    "guardian_vpn_1" IN UNNEST(stripe_subscriptions_history.product_capabilities)
    OR "guardian_vpn_1" IN UNNEST(stripe_subscriptions_history.plan_capabilities)
),
apple_iap_subscriptions AS (
  SELECT
    users.user_id,
    subplat.customer_id,
    subplat.subscription_id,
    subplat.original_subscription_id,
    subplat.plan_id,
    subplat.status,
    subplat.event_timestamp,
    subplat.subscription_start_date,
    -- Until the upgrade event surfacing work, original_subscription_start_date is set to be NULL
    CAST(NULL AS TIMESTAMP) AS original_subscription_start_date,
    CAST(NULL AS STRING) AS subscription_start_reason,
    subplat.created,
    subplat.trial_start,
    subplat.trial_end,
    CAST(NULL AS TIMESTAMP) AS canceled_at,
    CAST(NULL AS STRING) AS canceled_for_customer_at,
    CAST(NULL AS TIMESTAMP) AS cancel_at,
    subplat.cancel_at_period_end,
    IF(
      subplat.ended_at < TIMESTAMP(CURRENT_DATE),
      subplat.ended_at,
      CAST(NULL AS TIMESTAMP)
    ) AS ended_at,
    subplat.ended_reason,
    subplat.fxa_uid,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS country_name,
    CAST(NULL AS STRING) AS state,
    users.user_registration_date,
    subplat.provider,
    CAST(NULL AS INT64) AS plan_amount,
    CAST(NULL AS STRING) AS billing_scheme,
    CAST(NULL AS STRING) AS plan_currency,
    subplat.plan_interval,
    subplat.plan_interval_count,
    subplat.plan_interval_timezone,
    subplat.product_id,
    "Mozilla VPN" AS product_name,
    CONCAT(subplat.plan_interval_count, "-", subplat.plan_interval, "-", "apple") AS pricing_plan,
    subplat.billing_grace_period,
    CAST(NULL AS BOOL) AS has_refunds,
    CAST(NULL AS BOOL) AS has_fraudulent_charges,
    CAST(NULL AS BOOL) AS has_fraudulent_charge_refunds,
    subplat.promotion_codes,
    CAST(NULL AS INT64) AS promotion_discounts_amount,
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.apple_subscriptions_v1 AS subplat
  LEFT JOIN
    users
  USING
    (fxa_uid)
  WHERE
    subplat.product_id = "org.mozilla.ios.FirefoxVPN"
    AND subplat.fxa_uid IS NOT NULL
),
google_iap_subscriptions AS (
  SELECT
    users.user_id,
    subscriptions.customer_id,
    subscriptions.subscription_id,
    subscriptions.original_subscription_id,
    subscriptions.plan_id,
    CAST(NULL AS STRING) AS status,
    subscriptions.event_timestamp,
    subscriptions.subscription_start AS subscription_start_date,
    -- Until the upgrade event surfacing work, original_subscription_start_date is set to be NULL
    CAST(NULL AS TIMESTAMP) AS original_subscription_start_date,
    CAST(NULL AS STRING) AS subscription_start_reason,
    subscriptions.created,
    subscriptions.trial_start,
    subscriptions.trial_end,
    CAST(NULL AS TIMESTAMP) AS canceled_at,
    subscriptions.canceled_for_customer_at,
    CAST(NULL AS TIMESTAMP) AS cancel_at,
    CAST(NULL AS BOOL) AS cancel_at_period_end,
    IF(
      subscriptions.in_billing_grace_period,
      -- Google subscriptions in grace period have not ended
      CAST(NULL AS TIMESTAMP),
      IF(
        subscriptions.subscription_end < TIMESTAMP(CURRENT_DATE),
        subscriptions.subscription_end,
        CAST(NULL AS TIMESTAMP)
      )
    ) AS ended_at,
    CAST(NULL AS STRING) AS ended_reason,
    subscriptions.fxa_uid,
    subscriptions.country,
    standardized_country.country_name,
    CAST(NULL AS STRING) AS state,
    users.user_registration_date,
    subscriptions.provider,
    subscriptions.plan_amount,
    CAST(NULL AS STRING) AS billing_scheme,
    subscriptions.plan_currency,
    subscriptions.plan_interval,
    subscriptions.plan_interval_count,
    subscriptions.plan_interval_timezone,
    subscriptions.product_id,
    "Mozilla VPN" AS product_name,
    CONCAT(
      subscriptions.plan_interval_count,
      "-",
      subscriptions.plan_interval,
      "-",
      subscriptions.plan_currency,
      "-",
      (subscriptions.plan_amount / 100)
    ) AS pricing_plan,
    subscriptions.billing_grace_period,
    CAST(NULL AS BOOL) AS has_refunds,
    CAST(NULL AS BOOL) AS has_fraudulent_charges,
    CAST(NULL AS BOOL) AS has_fraudulent_charge_refunds,
    CAST(NULL AS ARRAY<STRING>) AS promotion_codes,
    CAST(NULL AS INT64) AS promotion_discounts_amount,
  FROM
    `moz-fx-data-shared-prod`.subscription_platform_derived.google_subscriptions_v1 AS subscriptions
  LEFT JOIN
    standardized_country
  USING
    (country)
  LEFT JOIN
    users
  USING
    (fxa_uid)
  WHERE
    subscriptions.product_id = "org.mozilla.firefox.vpn"
),
all_subscriptions AS (
  SELECT
    *
  FROM
    stripe_subscriptions
  UNION ALL
  SELECT
    *
  FROM
    apple_iap_subscriptions
  UNION ALL
  SELECT
    *
  FROM
    google_iap_subscriptions
),
subscription_last_touch_attributions AS (
  -- Select the latest attribution before the subscription originally started.
  SELECT
    subscriptions.subscription_id,
    ARRAY_AGG(
      fxa_attributions.attribution
      ORDER BY
        fxa_attributions.attribution.timestamp DESC
      LIMIT
        1
    )[ORDINAL(1)].*
  FROM
    all_subscriptions AS subscriptions
  JOIN
    fxa_attributions
  ON
    subscriptions.fxa_uid = fxa_attributions.fxa_uid
    AND COALESCE(
      subscriptions.original_subscription_start_date,
      subscriptions.subscription_start_date,
      subscriptions.trial_start
    ) >= fxa_attributions.attribution.timestamp
  GROUP BY
    subscription_id
),
all_subscriptions_with_attribution AS (
  SELECT
    subscriptions.*,
    attributions.timestamp AS attribution_timestamp,
    attributions.entrypoint_experiment,
    attributions.entrypoint_variation,
    attributions.utm_campaign,
    attributions.utm_content,
    attributions.utm_medium,
    attributions.utm_source,
    attributions.utm_term,
    mozfun.norm.vpn_attribution(
      utm_campaign => attributions.utm_campaign,
      utm_content => attributions.utm_content,
      utm_medium => attributions.utm_medium,
      utm_source => attributions.utm_source
    ).*
  FROM
    all_subscriptions AS subscriptions
  LEFT JOIN
    subscription_last_touch_attributions AS attributions
  ON
    subscriptions.subscription_id = attributions.subscription_id
),
all_subscriptions_with_end_date AS (
  SELECT
    *,
    IF(
      customer_id IS NOT NULL,
      MIN(subscription_start_date) OVER (PARTITION BY customer_id),
      subscription_start_date
    ) AS customer_start_date,
    COALESCE(ended_at, TIMESTAMP(CURRENT_DATE)) AS end_date,
  FROM
    all_subscriptions_with_attribution
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
      WHEN provider = "Apple Store"
        THEN "Cancelled by IAP"
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
  all_subscriptions_with_end_date
