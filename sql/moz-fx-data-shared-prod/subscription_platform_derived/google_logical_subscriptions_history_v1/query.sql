CREATE TEMP FUNCTION make_introductory_price_interval(period STRING, cycles INTEGER)
RETURNS INTERVAL AS (
  -- Convert the period from an ISO 8601 duration string into a BigQuery interval.
  MAKE_INTERVAL(
    year => COALESCE(CAST(REGEXP_EXTRACT(period, r'(\d+)Y') AS INTEGER), 0),
    month => COALESCE(CAST(REGEXP_EXTRACT(period, r'(\d+)M') AS INTEGER), 0),
    day => (
      COALESCE(CAST(REGEXP_EXTRACT(period, r'(\d+)D') AS INTEGER), 0) + (
        COALESCE(CAST(REGEXP_EXTRACT(period, r'(\d+)W') AS INTEGER), 0) * 7
      )
    )
  ) * cycles
);

WITH subscriptions_history AS (
  SELECT
    id,
    valid_from,
    valid_to,
    original_subscription_purchase_token,
    subscription,
    -- This should be kept in agreement with what SubPlat considers an active Google subscription:
    -- https://github.com/mozilla/fxa/blob/980012f04812d39cc655b626bad1b93d63df0fea/packages/fxa-shared/payments/iap/google-play/subscription-purchase.ts#L151-L154
    -- Since `google_subscriptions_history_v1` filters out replaced subscription records we don't need to check that here.
    (subscription.expiry_time > valid_from) AS subscription_is_active,
    -- Not all Google subscription records have `user_id` values, so we fall back to trying to get
    -- the Mozilla Account ID from the subscription's other nearby records.
    COALESCE(
      subscription.metadata.user_id,
      FIRST_VALUE(subscription.metadata.user_id IGNORE NULLS) OVER (
        PARTITION BY
          original_subscription_purchase_token
        ORDER BY
          valid_from,
          valid_to
        ROWS BETWEEN
          1 FOLLOWING
          AND UNBOUNDED FOLLOWING
      ),
      LAST_VALUE(subscription.metadata.user_id IGNORE NULLS) OVER (
        PARTITION BY
          original_subscription_purchase_token
        ORDER BY
          valid_from,
          valid_to
        ROWS BETWEEN
          UNBOUNDED PRECEDING
          AND 1 PRECEDING
      )
    ) AS mozilla_account_id,
    COALESCE(
      CAST(REGEXP_EXTRACT(subscription.metadata.sku, r'(\d+)[_-]?month') AS INTEGER),
      (CAST(REGEXP_EXTRACT(subscription.metadata.sku, r'(\d+)[_-]?year') AS INTEGER) * 12)
    ) AS plan_interval_months,
    (CAST(subscription.price_amount_micros AS DECIMAL) / 1000000) AS plan_amount,
    (
      CAST(
        subscription.introductory_price_info.introductory_price_amount_micros AS DECIMAL
      ) / 1000000
    ) AS introductory_price_amount,
    IF(
      subscription.introductory_price_info.introductory_price_period LIKE 'P%',
      (
        subscription.start_time + make_introductory_price_interval(
          subscription.introductory_price_info.introductory_price_period,
          subscription.introductory_price_info.introductory_price_cycles
        )
      ),
      NULL
    ) AS introductory_price_ends_at
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.google_subscriptions_history_v1`
),
subscription_starts AS (
  SELECT
    CONCAT(
      'Google-',
      original_subscription_purchase_token,
      '-',
      FORMAT_TIMESTAMP('%FT%H:%M:%E6S', valid_from)
    ) AS subscription_id,
    original_subscription_purchase_token,
    valid_from AS started_at,
    valid_to AS next_subscription_change_at,
    IF(
      valid_from = subscription.start_time
      OR valid_from < introductory_price_ends_at,
      subscription.promotion_code,
      NULL
    ) AS initial_discount_promotion_code
  FROM
    subscriptions_history
  QUALIFY
    subscription_is_active IS TRUE
    AND (
      LAG(subscription_is_active) OVER (
        PARTITION BY
          original_subscription_purchase_token
        ORDER BY
          valid_from,
          valid_to
      )
    ) IS NOT TRUE
),
subscriptions_history_periods AS (
  SELECT
    subscription_id,
    original_subscription_purchase_token,
    started_at,
    COALESCE(
      LEAD(started_at) OVER (
        PARTITION BY
          original_subscription_purchase_token
        ORDER BY
          started_at,
          next_subscription_change_at
      ),
      '9999-12-31 23:59:59.999999'
    ) AS ended_at,
    initial_discount_promotion_code
  FROM
    subscription_starts
),
google_sku_services AS (
  SELECT
    google_sku_id,
    ARRAY_AGG(
      STRUCT(services.id, services.name, tier.name AS tier)
      ORDER BY
        services.id
    ) AS services
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.services_v1` AS services
  CROSS JOIN
    UNNEST(services.google_sku_ids) AS google_sku_id
  LEFT JOIN
    UNNEST(services.tiers) AS tier
    ON google_sku_id IN UNNEST(tier.google_sku_ids)
  GROUP BY
    google_sku_id
),
google_sku_stripe_plans AS (
  SELECT
    google_sku_id,
    stripe_plans.interval AS plan_interval_type,
    stripe_plans.interval_count AS plan_interval_count,
    stripe_products.name AS product_name
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_plans_v1` AS stripe_plans
  CROSS JOIN
    UNNEST(stripe_plans.google_sku_ids) AS google_sku_id
  JOIN
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_products_v1` AS stripe_products
    ON stripe_plans.product_id = stripe_products.id
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY google_sku_id ORDER BY stripe_plans.created DESC)
)
SELECT
  CONCAT(
    history_period.subscription_id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', history.valid_from)
  ) AS id,
  history.valid_from,
  history.valid_to,
  history.id AS provider_subscriptions_history_id,
  (
    SELECT AS STRUCT
      history_period.subscription_id AS id,
      'Google' AS provider,
      'Google' AS payment_provider,
      history.subscription.metadata.purchase_token AS provider_subscription_id,
      CAST(NULL AS STRING) AS provider_subscription_item_id,
      history.subscription.start_time AS provider_subscription_created_at,
      CAST(NULL AS STRING) AS provider_customer_id,
      history.mozilla_account_id,
      TO_HEX(SHA256(history.mozilla_account_id)) AS mozilla_account_id_sha256,
      history.subscription.country_code,
      google_sku_services.services,
      history.subscription.metadata.package_name AS provider_product_id,
      google_sku_stripe_plans.product_name,
      history.subscription.metadata.sku AS provider_plan_id,
      IF(
        plan_interval_months IS NOT NULL,
        IF(
          MOD(plan_interval_months, 12) = 0,
          STRUCT(
            'year' AS plan_interval_type,
            DIV(plan_interval_months, 12) AS plan_interval_count
          ),
          STRUCT('month' AS plan_interval_type, plan_interval_months AS plan_interval_count)
        ),
        STRUCT(
          google_sku_stripe_plans.plan_interval_type,
          google_sku_stripe_plans.plan_interval_count
        )
      ).*,
      history.subscription.price_currency_code AS plan_currency,
      history.plan_amount,
      IF(ARRAY_LENGTH(google_sku_services.services) > 1, TRUE, FALSE) AS is_bundle,
      IF(history.subscription.payment_state = 2, TRUE, FALSE) AS is_trial,
      history.subscription_is_active AS is_active,
      -- The Google Play `purchases.subscriptions` API that SubPlat currently uses doesn't have
      -- a comprehensive subscription status field, so instead this mimics what Google Play's
      -- newer `purchases.subscriptionsv2` API would return in its `subscriptionState` field.
      -- https://developers.google.com/android-publisher/api-ref/rest/v3/purchases.subscriptionsv2#SubscriptionState
      IF(
        history.subscription_is_active,
        CASE
          WHEN history.subscription.auto_renewing
            AND history.subscription.payment_state = 0  -- Payment pending
            AND subscription.metadata.latest_notification_type = 6  -- SUBSCRIPTION_IN_GRACE_PERIOD
            THEN 'SUBSCRIPTION_STATE_IN_GRACE_PERIOD'
          WHEN history.subscription.payment_state = 0  -- Payment pending
            THEN 'SUBSCRIPTION_STATE_PENDING'
          WHEN history.subscription.metadata.latest_notification_type = 20  -- SUBSCRIPTION_PENDING_PURCHASE_CANCELED
            THEN 'SUBSCRIPTION_STATE_PENDING_PURCHASE_CANCELED'
          WHEN history.subscription.auto_renewing IS NOT TRUE
            THEN 'SUBSCRIPTION_STATE_CANCELED'
          ELSE 'SUBSCRIPTION_STATE_ACTIVE'
        END,
        CASE
          WHEN history.subscription.auto_renewing
            AND history.subscription.auto_resume_time IS NOT NULL
            THEN 'SUBSCRIPTION_STATE_PAUSED'
          WHEN history.subscription.auto_renewing
            AND history.subscription.metadata.latest_notification_type = 5  -- SUBSCRIPTION_ON_HOLD
            THEN 'SUBSCRIPTION_STATE_ON_HOLD'
          ELSE 'SUBSCRIPTION_STATE_EXPIRED'
        END
      ) AS provider_status,
      history_period.started_at,
      IF(
        history.subscription_is_active IS NOT TRUE,
        history.subscription.expiry_time,
        NULL
      ) AS ended_at,
      CAST(NULL AS TIMESTAMP) AS current_period_started_at,
      IF(
        history.subscription_is_active,
        history.subscription.expiry_time,
        NULL
      ) AS current_period_ends_at,
      history.subscription.auto_renewing IS TRUE AS auto_renew,
      IF(
        history.subscription.auto_renewing IS NOT TRUE,
        history.subscription.user_cancellation_time,
        NULL
      ) AS auto_renew_disabled_at,
      CAST(NULL AS STRING) AS initial_discount_name,
      history_period.initial_discount_promotion_code,
      CAST(NULL AS STRING) AS current_period_discount_name,
      IF(
        history.introductory_price_ends_at > history.valid_from,
        STRUCT(
          history.subscription.promotion_code AS current_period_discount_promotion_code,
          (
            history.plan_amount - history.introductory_price_amount
          ) AS current_period_discount_amount
        ),
        STRUCT(NULL AS current_period_discount_promotion_code, 0 AS current_period_discount_amount)
      ).*,
      CAST(NULL AS STRING) AS ongoing_discount_name,
      IF(
        history.introductory_price_ends_at > history.subscription.expiry_time,
        STRUCT(
          history.subscription.promotion_code AS ongoing_discount_promotion_code,
          (history.plan_amount - history.introductory_price_amount) AS ongoing_discount_amount,
          history.introductory_price_ends_at AS ongoing_discount_ends_at
        ),
        STRUCT(
          NULL AS ongoing_discount_promotion_code,
          0 AS ongoing_discount_amount,
          NULL AS ongoing_discount_ends_at
        )
      ).*,
      CAST(NULL AS BOOL) AS has_refunds,
      CAST(NULL AS BOOL) AS has_fraudulent_charges
  ) AS subscription
FROM
  subscriptions_history AS history
JOIN
  subscriptions_history_periods AS history_period
  ON history.original_subscription_purchase_token = history_period.original_subscription_purchase_token
  AND history.valid_from >= history_period.started_at
  AND history.valid_from < history_period.ended_at
LEFT JOIN
  google_sku_services
  ON LOWER(history.subscription.metadata.sku) = LOWER(google_sku_services.google_sku_id)
LEFT JOIN
  google_sku_stripe_plans
  ON LOWER(history.subscription.metadata.sku) = LOWER(google_sku_stripe_plans.google_sku_id)
