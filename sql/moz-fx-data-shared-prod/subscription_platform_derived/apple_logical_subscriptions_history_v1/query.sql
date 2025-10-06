WITH subscriptions_history AS (
  SELECT
    id,
    valid_from,
    valid_to,
    subscription,
    -- This should be kept in agreement with what SubPlat considers an active Apple subscription:
    -- https://github.com/mozilla/fxa/blob/e73499ee02cfc67a81e3f4cb21742fb95e00d1ea/packages/fxa-shared/payments/iap/apple-app-store/subscription-purchase.ts#L219-L224
    subscription.status IN (
      1,  -- 1 = active
      4   -- 4 = in billing grace period
    ) AS subscription_is_active,
    -- Not all Apple subscription records have `user_id` values, so we fall back to trying to get
    -- the Mozilla Account ID from the subscription's other nearby records.
    COALESCE(
      subscription.metadata.user_id,
      FIRST_VALUE(
        subscription.metadata.user_id IGNORE NULLS
      ) OVER following_subscription_changes_asc,
      LAST_VALUE(subscription.metadata.user_id IGNORE NULLS) OVER preceding_subscription_changes_asc
    ) AS mozilla_account_id,
    -- Apple subscription records prior to 2024-10-30 don't have `storefront` values (FXA-10549),
    -- so we fall back to trying to get it from following records.
    COALESCE(
      subscription.last_transaction.storefront,
      FIRST_VALUE(
        subscription.last_transaction.storefront IGNORE NULLS
      ) OVER following_subscription_changes_asc
    ) AS storefront,
    COALESCE(
      CAST(
        REGEXP_EXTRACT(
          subscription.last_transaction.product_id,
          r'(\d+)[_.]?mo(?:nth|[_.]|$)'
        ) AS INTEGER
      ),
      (
        CAST(
          REGEXP_EXTRACT(subscription.last_transaction.product_id, r'(\d+)[_.]?year') AS INTEGER
        ) * 12
      )
    ) AS plan_interval_months,
    IF(
      subscription.renewal_info.auto_renew_status = 0  -- 0 = auto-renewal off
      AND subscription.renewal_info.auto_renew_status IS DISTINCT FROM (
        LAG(subscription.renewal_info.auto_renew_status) OVER subscription_changes_asc
      ),
      valid_from,
      NULL
    ) AS auto_renew_disabled_at
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.apple_subscriptions_history_v1`
  WHERE
    subscription.last_transaction.in_app_ownership_type = 'PURCHASED'
    AND valid_to > valid_from
  WINDOW
    subscription_changes_asc AS (
      PARTITION BY
        subscription.original_transaction_id
      ORDER BY
        valid_from,
        valid_to
    ),
    preceding_subscription_changes_asc AS (
      PARTITION BY
        subscription.original_transaction_id
      ORDER BY
        valid_from,
        valid_to
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND 1 PRECEDING
    ),
    following_subscription_changes_asc AS (
      PARTITION BY
        subscription.original_transaction_id
      ORDER BY
        valid_from,
        valid_to
      ROWS BETWEEN
        1 FOLLOWING
        AND UNBOUNDED FOLLOWING
    )
),
subscription_starts AS (
  SELECT
    CONCAT(
      'Apple-',
      subscription.original_transaction_id,
      '-',
      FORMAT_TIMESTAMP('%FT%H:%M:%E6S', valid_from)
    ) AS subscription_id,
    subscription.original_transaction_id,
    valid_from AS started_at,
    valid_to AS next_subscription_change_at,
    subscription.last_transaction.offer_identifier AS initial_discount_promotion_code
  FROM
    subscriptions_history
  QUALIFY
    subscription_is_active IS TRUE
    AND (
      LAG(subscription_is_active) OVER (
        PARTITION BY
          subscription.original_transaction_id
        ORDER BY
          valid_from,
          valid_to
      )
    ) IS NOT TRUE
),
subscriptions_history_periods AS (
  SELECT
    subscription_id,
    original_transaction_id,
    started_at,
    COALESCE(
      LEAD(started_at) OVER (
        PARTITION BY
          original_transaction_id
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
apple_product_services AS (
  SELECT
    apple_product_id,
    ARRAY_AGG(
      STRUCT(services.id, services.name, tier.name AS tier)
      ORDER BY
        services.id
    ) AS services
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.services_v1` AS services
  CROSS JOIN
    UNNEST(services.apple_product_ids) AS apple_product_id
  LEFT JOIN
    UNNEST(services.tiers) AS tier
    ON apple_product_id IN UNNEST(tier.apple_product_ids)
  GROUP BY
    apple_product_id
),
apple_product_stripe_plans AS (
  SELECT
    apple_product_id,
    stripe_plans.interval AS plan_interval_type,
    stripe_plans.interval_count AS plan_interval_count,
    UPPER(stripe_plans.currency) AS plan_currency,
    (CAST(stripe_plans.amount AS DECIMAL) / 100) AS plan_amount,
    stripe_products.name AS product_name
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_plans_v1` AS stripe_plans
  CROSS JOIN
    UNNEST(stripe_plans.apple_product_ids) AS apple_product_id
  JOIN
    `moz-fx-data-shared-prod.subscription_platform_derived.stripe_products_v1` AS stripe_products
    ON stripe_plans.product_id = stripe_products.id
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY apple_product_id ORDER BY stripe_plans.created DESC)
),
historical_product_prices AS (
  -- Get the historical product prices as they were around when SubPlat started recording the
  -- `storefront`, `currency`, and `price` fields for Apple subscriptions on 2024-10-30 (FXA-10549).
  SELECT
    subscription.last_transaction.product_id,
    subscription.last_transaction.storefront,
    subscription.last_transaction.currency,
    subscription.last_transaction.price
  FROM
    subscriptions_history
  WHERE
    subscription_is_active
    AND subscription.last_transaction.storefront IS NOT NULL
    AND subscription.last_transaction.currency IS NOT NULL
    AND subscription.last_transaction.price > 0
  QUALIFY
    1 = ROW_NUMBER() OVER (
      PARTITION BY
        subscription.last_transaction.product_id,
        subscription.last_transaction.storefront
      ORDER BY
        valid_from,
        id
    )
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
  STRUCT(
    history_period.subscription_id AS id,
    'Apple' AS provider,
    'Apple' AS payment_provider,
    history.subscription.original_transaction_id AS provider_subscription_id,
    CAST(NULL AS STRING) AS provider_subscription_item_id,
    history.subscription.last_transaction.original_purchase_date AS provider_subscription_created_at,
    CAST(NULL AS STRING) AS provider_customer_id,
    history.mozilla_account_id,
    TO_HEX(SHA256(history.mozilla_account_id)) AS mozilla_account_id_sha256,
    country_codes.code AS country_code,
    apple_product_services.services,
    history.subscription.bundle_id AS provider_product_id,
    apple_product_stripe_plans.product_name,
    history.subscription.last_transaction.product_id AS provider_plan_id,
    IF(
      history.plan_interval_months IS NOT NULL,
      IF(MOD(history.plan_interval_months, 12) = 0, 'year', 'month'),
      apple_product_stripe_plans.plan_interval_type
    ) AS plan_interval_type,
    IF(
      history.plan_interval_months IS NOT NULL,
      IF(
        MOD(history.plan_interval_months, 12) = 0,
        DIV(history.plan_interval_months, 12),
        history.plan_interval_months
      ),
      apple_product_stripe_plans.plan_interval_count
    ) AS plan_interval_count,
    -- Apple subscription records prior to 2024-10-30 don't have `currency` or `price` values (FXA-10549),
    -- so we fall back to trying to get those from following subscription records for the same product,
    -- from the historical product prices from around 2024-10-30, and from the Stripe plans.
    COALESCE(
      history.subscription.last_transaction.currency,
      FIRST_VALUE(
        IF(
          subscription.last_transaction.price > 0,
          subscription.last_transaction.currency,
          NULL
        ) IGNORE NULLS
      ) OVER following_subscription_product_changes_asc,
      historical_product_prices.currency,
      apple_product_stripe_plans.plan_currency
    ) AS plan_currency,
    COALESCE(
      (
        CAST(
          COALESCE(
            history.subscription.last_transaction.price,
            FIRST_VALUE(
              NULLIF(subscription.last_transaction.price, 0) IGNORE NULLS
            ) OVER following_subscription_product_changes_asc,
            historical_product_prices.price
          ) AS DECIMAL
        ) / 1000
      ),
      apple_product_stripe_plans.plan_amount
    ) AS plan_amount,
    IF(ARRAY_LENGTH(apple_product_services.services) > 1, TRUE, FALSE) AS is_bundle,
    -- SubPlat had a bug where it wasn't removing `offerType` fields from trial subscription records when
    -- the trial ended (FXA-7118), which we work around by presuming that trials are less than a month.
    IF(
      history.subscription.last_transaction.offer_type = 1  -- 1 = introductory offer
      AND DATETIME_DIFF(
        DATETIME(history.subscription.last_transaction.expires_date, 'America/Los_Angeles'),
        DATETIME(history.subscription.last_transaction.purchase_date, 'America/Los_Angeles'),
        DAY
      ) < 28,
      TRUE,
      FALSE
    ) AS is_trial,
    history.subscription_is_active AS is_active,
    -- https://developer.apple.com/documentation/appstoreserverapi/status
    -- https://github.com/agisboye/app-store-server-api/blob/feed66572ebafa407f08d9d6cbe626cc5f9ded67/src/Models.ts#L198-L204
    CASE
      history.subscription.status
      WHEN 1
        THEN 'Active'
      WHEN 2
        THEN 'Expired'
      WHEN 3
        THEN 'InBillingRetry'
      WHEN 4
        THEN 'InBillingGracePeriod'
      WHEN 5
        THEN 'Revoked'
    END AS provider_status,
    history_period.started_at,
    IF(
      history.subscription_is_active IS NOT TRUE,
      IF(
        history.subscription.status = 5,  -- 5 = revoked
        COALESCE(
          history.subscription.last_transaction.revocation_date,
          history.subscription.last_transaction.expires_date
        ),
        COALESCE(
          history.subscription.renewal_info.grace_period_expires_date,
          history.subscription.last_transaction.expires_date
        )
      ),
      NULL
    ) AS ended_at,
    -- API Docs enumerations :
    -- https://developer.apple.com/documentation/appstoreserverapi/status
    -- https://developer.apple.com/documentation/appstoreserverapi/expirationintent
    CASE
      WHEN history.subscription_is_active
        THEN NULL
      WHEN (
          history.subscription.status = 2 -- 2 = auto-renewable subscription is expired
          AND history.subscription.renewal_info.expiration_intent IN (
            1,
            3
          ) -- 1 = customer canceled their subscription -- 3 = customer didn’t consent to an auto-renewable subscription
        )
        -- admins are not revoking Apple subscriptions so we can assume such cases are from users
        OR history.subscription.status = 5 -- 5 = auto-renewable subscription is revoked
        THEN 'User Initiated'
      WHEN (
          history.subscription.status = 2 -- 2 = auto-renewable subscription is expired
          AND history.subscription.renewal_info.expiration_intent = 2 -- 2 = Billing error
        )
        OR history.subscription.status = 3 -- 3 = auto-renewable subscription is in a billing retry period
        THEN 'Payment Failure'
      ELSE 'Other'
    END AS ended_reason,
    IF(
      history.subscription_is_active,
      history.subscription.last_transaction.purchase_date,
      NULL
    ) AS current_period_started_at,
    IF(
      history.subscription_is_active,
      history.subscription.last_transaction.expires_date,
      NULL
    ) AS current_period_ends_at,
    IF(
      history.subscription.renewal_info.auto_renew_status = 1,  -- 1 = auto-renewal on
      TRUE,
      FALSE
    ) AS auto_renew,
    IF(
      history.subscription.renewal_info.auto_renew_status = 0,  -- 0 = auto-renewal off
      COALESCE(
        history.auto_renew_disabled_at,
        LAST_VALUE(
          history.auto_renew_disabled_at IGNORE NULLS
        ) OVER preceding_subscription_changes_asc,
        history_period.started_at
      ),
      NULL
    ) AS auto_renew_disabled_at,
    CAST(NULL AS STRING) AS initial_discount_name,
    history_period.initial_discount_promotion_code,
    CAST(NULL AS STRING) AS current_period_discount_name,
    history.subscription.last_transaction.offer_identifier AS current_period_discount_promotion_code,
    IF(
      history.subscription.last_transaction.offer_identifier IS NOT NULL,
      -- Unfortunately we have no way to know what the actual discount amount is.
      CAST(NULL AS DECIMAL),
      0
    ) AS current_period_discount_amount,
    CAST(NULL AS STRING) AS ongoing_discount_name,
    history.subscription.renewal_info.offer_identifier AS ongoing_discount_promotion_code,
    IF(
      history.subscription.renewal_info.offer_identifier IS NOT NULL,
      -- Unfortunately we have no way to know what the actual discount amount is.
      CAST(NULL AS DECIMAL),
      0
    ) AS ongoing_discount_amount,
    CAST(NULL AS TIMESTAMP) AS ongoing_discount_ends_at,
    IF(
      history.subscription.status = 5,  -- 5 = revoked
      TRUE,
      FALSE
    ) AS has_refunds,
    CAST(NULL AS BOOL) AS has_fraudulent_charges
  ) AS subscription
FROM
  subscriptions_history AS history
JOIN
  subscriptions_history_periods AS history_period
  ON history.subscription.original_transaction_id = history_period.original_transaction_id
  AND history.valid_from >= history_period.started_at
  AND history.valid_from < history_period.ended_at
LEFT JOIN
  apple_product_services
  ON LOWER(history.subscription.last_transaction.product_id) = LOWER(
    apple_product_services.apple_product_id
  )
LEFT JOIN
  apple_product_stripe_plans
  ON LOWER(history.subscription.last_transaction.product_id) = LOWER(
    apple_product_stripe_plans.apple_product_id
  )
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS country_codes
  ON history.storefront = country_codes.code_3
LEFT JOIN
  historical_product_prices
  ON history.subscription.last_transaction.product_id = historical_product_prices.product_id
  AND history.storefront = historical_product_prices.storefront
WINDOW
  preceding_subscription_changes_asc AS (
    PARTITION BY
      history_period.subscription_id
    ORDER BY
      history.valid_from,
      history.valid_to
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND 1 PRECEDING
  ),
  following_subscription_product_changes_asc AS (
    PARTITION BY
      history_period.subscription_id,
      history.subscription.last_transaction.product_id
    ORDER BY
      history.valid_from,
      history.valid_to
    ROWS BETWEEN
      1 FOLLOWING
      AND UNBOUNDED FOLLOWING
  )
