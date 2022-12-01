WITH standardized_country AS (
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
    mozdata.subscription_platform.stripe_subscriptions_history
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
  LEFT JOIN
    attribution
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
    users.user_registration_date,
    attribution.entrypoint_experiment,
    attribution.entrypoint_variation,
    attribution.utm_campaign,
    attribution.utm_content,
    attribution.utm_medium,
    attribution.utm_source,
    attribution.utm_term,
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
    subplat.promotion_codes,
    CAST(NULL AS INT64) AS promotion_discounts_amount,
  FROM
    mozdata.subscription_platform.apple_subscriptions AS subplat
  LEFT JOIN
    users
  USING
    (fxa_uid)
  LEFT JOIN
    attribution
  USING
    (fxa_uid)
  WHERE
    subplat.product_id = "org.mozilla.ios.FirefoxVPN"
    AND subplat.fxa_uid IS NOT NULL
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
    MAX(user_id) AS fxa_uid,
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
    document_id,
    IF(
      active_dates_offset > 0,
      document_id || "-" || active_dates_offset,
      document_id
    ) AS subscription_id,
    IF(active_dates_offset > 0, document_id, NULL) AS original_subscription_id,
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
        CONTAINS_SUBSTR(sku, ".1_month_subscription")
        OR CONTAINS_SUBSTR(sku, ".monthly")
      THEN
        STRUCT("month" AS plan_interval, 1 AS plan_interval_count)
      WHEN
        CONTAINS_SUBSTR(sku, ".6_month_subscription")
      THEN
        ("month", 6)
      WHEN
        CONTAINS_SUBSTR(sku, ".12_month_subscription")
      THEN
        ("year", 1)
      WHEN
        CONTAINS_SUBSTR(sku, ".1_day_subscription")
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
android_iap_trial_periods AS (
  SELECT
    document_id,
    start_time,
    MAX(expiry_time) AS end_time,
  FROM
    android_iap_events
  WHERE
    payment_state = 2
  GROUP BY
    document_id,
    start_time
),
android_iap_subscriptions AS (
  SELECT
    users.user_id,
    periods.fxa_uid AS customer_id,
    periods.subscription_id,
    periods.original_subscription_id,
    periods.plan_id,
    CAST(NULL AS STRING) AS status,
    periods.event_timestamp,
    IF(
      periods.end_time <= trial_periods.end_time,
      NULL,
      COALESCE(trial_periods.end_time, periods.start_time)
    ) AS subscription_start_date,
    CAST(NULL AS STRING) AS subscription_start_reason,
    periods.created,
    trial_periods.start_time AS trial_start,
    trial_periods.end_time AS trial_end,
    CAST(NULL AS TIMESTAMP) AS canceled_at,
    periods.canceled_for_customer_at,
    CAST(NULL AS TIMESTAMP) AS cancel_at,
    CAST(NULL AS BOOL) AS cancel_at_period_end,
    IF(
      periods.in_billing_grace_period,
      -- android subscriptions in grace period have not ended
      CAST(NULL AS TIMESTAMP),
      IF(periods.end_time < TIMESTAMP(CURRENT_DATE), periods.end_time, CAST(NULL AS TIMESTAMP))
    ) AS ended_at,
    CAST(NULL AS STRING) AS ended_reason,
    periods.fxa_uid,
    periods.country,
    standardized_country.country_name,
    users.user_registration_date,
    attribution.entrypoint_experiment,
    attribution.entrypoint_variation,
    attribution.utm_campaign,
    attribution.utm_content,
    attribution.utm_medium,
    attribution.utm_source,
    attribution.utm_term,
    "Google Play" AS provider,
    periods.plan_amount,
    CAST(NULL AS STRING) AS billing_scheme,
    periods.plan_currency,
    periods.plan_interval,
    periods.plan_interval_count,
    "Etc/UTC" AS plan_interval_timezone,
    periods.product_id,
    "Mozilla VPN" AS product_name,
    CONCAT(
      periods.plan_interval_count,
      "-",
      periods.plan_interval,
      "-",
      periods.plan_currency,
      "-",
      (periods.plan_amount / 100)
    ) AS pricing_plan,
    IF(periods.in_billing_grace_period, INTERVAL 1 MONTH, INTERVAL 0 DAY) AS billing_grace_period,
    CAST(NULL AS ARRAY<STRING>) AS promotion_codes,
    CAST(NULL AS INT64) AS promotion_discounts_amount,
  FROM
    android_iap_periods AS periods
  LEFT JOIN
    android_iap_trial_periods AS trial_periods
  USING
    (document_id, start_time)
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
    android_iap_subscriptions
),
vpn_subscriptions_with_end_date AS (
  SELECT
    *,
    IF(
      customer_id IS NOT NULL,
      MIN(subscription_start_date) OVER (PARTITION BY customer_id),
      subscription_start_date
    ) AS customer_start_date,
    COALESCE(ended_at, TIMESTAMP(CURRENT_DATE)) AS end_date,
  FROM
    vpn_subscriptions
)
SELECT
  * REPLACE (
    CASE
    WHEN
      subscription_start_date IS NULL
    THEN
      NULL
    WHEN
      subscription_start_reason IS NOT NULL
    THEN
      subscription_start_reason
    WHEN
      trial_start IS NOT NULL
    THEN
      "Converted Trial"
    WHEN
      DATE(subscription_start_date) = DATE(customer_start_date)
    THEN
      "New"
    ELSE
      "Resurrected"
    END
    AS subscription_start_reason,
    CASE
    WHEN
      ended_at IS NULL
    THEN
      NULL
    WHEN
      ended_reason IS NOT NULL
    THEN
      ended_reason
    WHEN
      provider = "Apple Store"
    THEN
      "Cancelled by IAP"
    WHEN
      canceled_for_customer_at IS NOT NULL
      OR cancel_at_period_end
    THEN
      "Cancelled by Customer"
    ELSE
      "Payment Failed"
    END
    AS ended_reason
  ),
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
  vpn_subscriptions_with_end_date
