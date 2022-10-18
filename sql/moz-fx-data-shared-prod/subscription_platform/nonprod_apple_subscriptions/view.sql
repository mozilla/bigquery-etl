CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform.nonprod_apple_subscriptions`
AS
WITH apple_iap_events AS (
  SELECT
    document_id,
    CAST(NULL AS STRING) AS legacy_subscription_id,
    `timestamp` AS event_timestamp,
    mozfun.iap.parse_apple_event(`data`).*,
  FROM
    `moz-fx-fxa-nonprod-375e.firestore_export.iap_app_store_purchases_raw_changelog`
),
apple_iap_aggregates AS (
  SELECT
    original_transaction_id,
    COALESCE(
      ANY_VALUE(legacy_subscription_id),
      original_transaction_id
    ) AS original_subscription_id,
    MIN(original_purchase_date) AS original_purchase_date,
    MAX(user_id) AS fxa_uid,
    ARRAY_CONCAT(
      -- original_purchase_date does not change
      [MIN(original_purchase_date)],
      -- status 1 means subscription is active
      IFNULL(
        ARRAY_AGG(
          DISTINCT IF(purchase_date < expires_date AND status = 1, purchase_date, NULL) IGNORE NULLS
        ),
        []
      )
    ) AS start_times,
    ARRAY_CONCAT(
      -- status 2 means subscription is expired
      IFNULL(ARRAY_AGG(DISTINCT IF(status = 2, expires_date, NULL) IGNORE NULLS), []),
      -- only use most recent expires_date when not limited to status 2
      ARRAY_AGG(expires_date ORDER BY verified_at DESC LIMIT 1)
    ) AS end_times,
    ARRAY_AGG(DISTINCT offer_identifier IGNORE NULLS) AS promotion_codes,
    ARRAY_AGG(
      STRUCT(
        bundle_id,
        expiration_intent,
        grace_period_expires_date,
        is_in_billing_retry,
        original_transaction_id,
        product_id,
        revocation_reason,
        status,
        verified_at
      )
      ORDER BY
        verified_at DESC
      LIMIT
        1
    )[OFFSET(0)].*,
  FROM
    apple_iap_events
  GROUP BY
    original_transaction_id
),
apple_iap_periods AS (
  SELECT
    original_transaction_id,
    legacy_subscription_id,
    original_purchase_date,
    fxa_uid,
    bundle_id,
    expiration_intent,
    original_transaction_id,
    product_id,
    revocation_reason,
    status,
    verified_at,
    IF(
      period_offset > 0,
      original_subscription_id || "-" || period_offset,
      original_subscription_id
    ) AS subscription_id,
    original_subscription_id,
    start_time,
    end_time,
    (
      is_in_billing_retry
      AND grace_period_expires_date > end_time
      -- only the last set of active dates can be in the billing grace period
      AND 1 = ROW_NUMBER() OVER (PARTITION BY original_transaction_id ORDER BY period_offset DESC)
    ) AS in_billing_grace_period,
    grace_period_expires_date,
    promotion_codes,
  FROM
    apple_iap_aggregates
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
    WITH OFFSET AS period_offset
),
apple_iap_trial_periods AS (
  SELECT
    original_transaction_id,
    MIN(original_purchase_date) AS start_time,
    MAX(expires_date) AS end_time,
  FROM
    apple_iap_events
  WHERE
    offer_type = 1
  GROUP BY
    original_transaction_id
)
SELECT
  periods.fxa_uid AS customer_id,
  periods.subscription_id,
  NULLIF(periods.original_subscription_id, periods.subscription_id) AS original_subscription_id,
  periods.product_id AS plan_id,
  CAST(periods.status AS STRING) AS status,
  periods.verified_at AS event_timestamp,
  IF(
    periods.end_time <= trial_periods.end_time,
    NULL,
    COALESCE(trial_periods.end_time, periods.start_time)
  ) AS subscription_start_date,
  periods.original_purchase_date AS created,
  trial_periods.start_time AS trial_start,
  trial_periods.end_time AS trial_end,
  periods.end_time AS ended_at,
  -- https://developer.apple.com/documentation/appstoreservernotifications/expirationintent
  CASE
    periods.expiration_intent
  WHEN
    periods.expiration_intent = 1
  THEN
    "Cancelled by Customer"
  WHEN
    periods.expiration_intent = 2
  THEN
    "Payment Failed"
  WHEN
    periods.expiration_intent = 3
  THEN
    "Price Change Not Approved by Customer"
  WHEN
    periods.expiration_intent = 4
  THEN
    "Product Unavailable at Renewal"
  WHEN
    periods.revocation_reason IS NOT NULL
  THEN
    "Refund"
  END
  AS ended_reason,
  periods.fxa_uid,
  "Apple Store" AS provider,
  (
    CASE
    WHEN
      CONTAINS_SUBSTR(periods.product_id, ".1_month_subscription")
    THEN
      STRUCT("month" AS plan_interval, 1 AS plan_interval_count)
    WHEN
      CONTAINS_SUBSTR(periods.product_id, ".6_mo_subscription")
    THEN
      ("month", 6)
    WHEN
      CONTAINS_SUBSTR(periods.product_id, ".1_year_subscription")
    THEN
      ("year", 1)
    ELSE
      ERROR("subscription period not found: " || periods.product_id)
    END
  ).*,
  "America/Los_Angeles" AS plan_interval_timezone,
  periods.bundle_id AS product_id,
  periods.in_billing_grace_period,
  IF(
    periods.in_billing_grace_period,
    periods.grace_period_expires_date - periods.end_time,
    NULL
  ) AS billing_grace_period,
  periods.promotion_codes,
FROM
  apple_iap_periods AS periods
LEFT JOIN
  apple_iap_trial_periods AS trial_periods
USING
  (document_id, start_time)
