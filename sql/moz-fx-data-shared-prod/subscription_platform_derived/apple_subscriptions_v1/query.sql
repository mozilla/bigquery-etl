WITH apple_iap_events AS (
  SELECT
    document_id,
    -- WARNING: field order from here must exactly match guardian_apple_events_v1
    `timestamp` AS event_timestamp,
    mozfun.iap.parse_apple_event(`data`).*,
  FROM
    `moz-fx-fxa-prod-0712.firestore_export.iap_app_store_purchases_raw_changelog`
  UNION ALL
  SELECT
    CAST(NULL AS STRING) AS document_id,
    *
  FROM
    `moz-fx-data-shared-prod`.mozilla_vpn_derived.guardian_apple_events_v1
  WHERE
    environment = "Production"
),
apple_iap_times AS (
  SELECT
    original_transaction_id,
    user_id,
    ARRAY_CONCAT(
      -- original_purchase_date does not change
      -- sometimes the earliest purchase_date is a few seconds before original_purchase_date
      [MIN(LEAST(original_purchase_date, purchase_date))],
      -- status 1 means subscription is active
      IFNULL(ARRAY_AGG(DISTINCT IF(status IN (1, 3, 4), purchase_date, NULL) IGNORE NULLS), [])
    ) AS start_times,
    ARRAY_CONCAT(
      -- status 2 means subscription is expired
      IFNULL(ARRAY_AGG(DISTINCT IF(status = 2, expires_date, NULL) IGNORE NULLS), []),
      -- status 5 means subscription is revoked
      IFNULL(ARRAY_AGG(DISTINCT IF(status = 5, revocation_date, NULL) IGNORE NULLS), []),
      -- only use most recent expires_date when not limited to status 2
      ARRAY_AGG(expires_date ORDER BY verified_at DESC LIMIT 1)
    ) AS end_times,
  FROM
    apple_iap_events
  GROUP BY
    original_transaction_id,
    user_id
),
apple_iap_periods AS (
  SELECT
    original_transaction_id,
    user_id,
    start_time,
    end_time,
    period_offset,
  FROM
    apple_iap_times
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
apple_iap_period_aggregates AS (
  SELECT
    periods.original_transaction_id,
    periods.user_id,
    periods.start_time,
    periods.end_time,
    periods.period_offset,
    periods.original_transaction_id AS original_subscription_id,
    MIN(events.original_purchase_date) AS original_purchase_date,
    ARRAY_AGG(DISTINCT events.offer_identifier IGNORE NULLS) AS promotion_codes,
    ARRAY_AGG(
      STRUCT(
        events.auto_renew_status,
        events.bundle_id,
        events.expiration_intent,
        events.is_in_billing_retry,
        events.grace_period_expires_date,
        events.product_id,
        events.revocation_reason,
        events.status,
        events.verified_at
      )
      ORDER BY
        events.verified_at DESC
      LIMIT
        1
    )[OFFSET(0)].*,
  FROM
    apple_iap_periods AS periods
  JOIN
    apple_iap_events AS events
    ON periods.original_transaction_id = events.original_transaction_id
    AND periods.user_id = events.user_id
    AND periods.start_time <= events.purchase_date
    AND periods.end_time > events.purchase_date
  GROUP BY
    periods.original_transaction_id,
    periods.user_id,
    periods.start_time,
    periods.end_time,
    periods.period_offset
),
apple_iap_enhanced_period_aggregates AS (
  SELECT
    *,
    IF(
      period_offset > 0,
      original_subscription_id || "-" || period_offset,
      original_subscription_id
    ) AS subscription_id,
    (
      (is_in_billing_retry OR grace_period_expires_date > end_time)
      -- only the last set of active dates can be in the billing grace period
      AND 1 = ROW_NUMBER() OVER (PARTITION BY original_transaction_id ORDER BY period_offset DESC)
    ) AS in_billing_grace_period,
  FROM
    apple_iap_period_aggregates
),
apple_iap_trial_periods AS (
  SELECT
    original_transaction_id,
    user_id,
    purchase_date AS start_time,
    expires_date AS end_time,
  FROM
    apple_iap_events
  WHERE
    offer_type = 1
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY original_transaction_id, user_id ORDER BY expires_date DESC)
)
SELECT
  periods.user_id AS customer_id,
  periods.subscription_id,
  NULLIF(periods.original_subscription_id, periods.subscription_id) AS original_subscription_id,
  periods.product_id AS plan_id,
  CASE
    periods.status
    WHEN 1
      THEN "active"
    WHEN 2
      THEN "expired"
    WHEN 3
      THEN "in billing retry period"
    WHEN 4
      THEN "in billing grace period"
    WHEN 5
      THEN "revoked"
  END AS status,
  periods.verified_at AS event_timestamp,
  IF(
    periods.end_time <= trial_periods.end_time,
    NULL,
    COALESCE(trial_periods.end_time, periods.start_time)
  ) AS subscription_start_date,
  periods.original_purchase_date AS created,
  trial_periods.start_time AS trial_start,
  trial_periods.end_time AS trial_end,
  periods.auto_renew_status = 0 AS cancel_at_period_end,
  periods.end_time AS ended_at,
  -- https://developer.apple.com/documentation/appstoreservernotifications/expirationintent
  CASE
    WHEN periods.expiration_intent = 1
      THEN "Cancelled by Customer"
    WHEN periods.expiration_intent = 2
      THEN "Payment Failed"
    WHEN periods.expiration_intent = 3
      THEN "Price Change Not Approved by Customer"
    WHEN periods.expiration_intent = 4
      THEN "Product Unavailable at Renewal"
    WHEN periods.revocation_reason IS NOT NULL
      THEN "Refund"
  END AS ended_reason,
  periods.user_id AS fxa_uid,
  "Apple Store" AS provider,
  (
    CASE
      WHEN CONTAINS_SUBSTR(periods.product_id, ".1_month_subscription")
        THEN STRUCT("month" AS plan_interval, 1 AS plan_interval_count)
      WHEN CONTAINS_SUBSTR(periods.product_id, ".6_mo_subscription")
        THEN ("month", 6)
      WHEN CONTAINS_SUBSTR(periods.product_id, ".1_year_subscription")
        THEN ("year", 1)
      ELSE ERROR("subscription period not found: " || periods.product_id)
    END
  ).*,
  "America/Los_Angeles" AS plan_interval_timezone,
  periods.bundle_id AS product_id,
  periods.in_billing_grace_period,
  IF(
    (periods.in_billing_grace_period AND periods.grace_period_expires_date > periods.end_time),
    periods.grace_period_expires_date - periods.end_time,
    INTERVAL 0 DAY
  ) AS billing_grace_period,
  periods.promotion_codes,
FROM
  apple_iap_enhanced_period_aggregates AS periods
LEFT JOIN
  apple_iap_trial_periods AS trial_periods
  USING (original_transaction_id, user_id)
