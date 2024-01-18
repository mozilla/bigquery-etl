WITH google_iap_events AS (
  SELECT
    document_id,
    NULLIF(`timestamp`, "1970-01-01 00:00:00") AS event_timestamp,
    mozfun.iap.parse_android_receipt(`data`).*
  FROM
    `moz-fx-fxa-prod-0712.firestore_export.iap_google_raw_changelog`
),
google_iap_aggregates AS (
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
    google_iap_events
  WHERE
    form_of_payment = "GOOGLE_PLAY"
  GROUP BY
    document_id
),
google_iap_periods AS (
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
        WHEN CONTAINS_SUBSTR(sku, ".1_month_subscription")
          OR CONTAINS_SUBSTR(sku, ".monthly")
          THEN STRUCT("month" AS plan_interval, 1 AS plan_interval_count)
        WHEN CONTAINS_SUBSTR(sku, ".6_month_subscription")
          THEN ("month", 6)
        WHEN CONTAINS_SUBSTR(sku, ".12_month_subscription")
          THEN ("year", 1)
        WHEN CONTAINS_SUBSTR(sku, ".1_day_subscription")
          -- only used for testing
          THEN ("day", 1)
      END
    ).*,
    (
      auto_renewing
      AND payment_state = 0
      -- only the last set of active dates can be in the billing grace period
      AND 1 = ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY active_dates_offset DESC)
    ) AS in_billing_grace_period,
  FROM
    google_iap_aggregates
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
google_iap_trial_periods AS (
  SELECT
    document_id,
    start_time,
    MAX(expiry_time) AS end_time,
  FROM
    google_iap_events
  WHERE
    payment_state = 2
  GROUP BY
    document_id,
    start_time
)
SELECT
  periods.fxa_uid AS customer_id,
  periods.subscription_id,
  periods.original_subscription_id,
  periods.plan_id,
  periods.event_timestamp,
  IF(
    periods.end_time <= trial_periods.end_time,
    NULL,
    COALESCE(trial_periods.end_time, periods.start_time)
  ) AS subscription_start,
  periods.created,
  trial_periods.start_time AS trial_start,
  trial_periods.end_time AS trial_end,
  periods.canceled_for_customer_at,
  periods.end_time AS subscription_end,
  periods.fxa_uid,
  periods.country,
  "Google Play" AS provider,
  periods.plan_amount,
  periods.plan_currency,
  periods.plan_interval,
  periods.plan_interval_count,
  "Etc/UTC" AS plan_interval_timezone,
  periods.product_id,
  periods.in_billing_grace_period,
  IF(periods.in_billing_grace_period, INTERVAL 1 MONTH, INTERVAL 0 DAY) AS billing_grace_period,
FROM
  google_iap_periods AS periods
LEFT JOIN
  google_iap_trial_periods AS trial_periods
  USING (document_id, start_time)
