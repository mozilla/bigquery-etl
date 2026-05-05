CREATE OR REPLACE FUNCTION iap.scrub_apple_receipt(apple_receipt ANY TYPE)
RETURNS STRUCT<
  environment STRING,
  active_period STRUCT<
    -- *_date fields deprecated in favor of TIMESTAMP fields
    start_date DATE,
    end_date DATE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    `interval` STRING,
    interval_count INT64
  >,
  trial_period STRUCT<start_time TIMESTAMP, end_time TIMESTAMP>
> AS (
  STRUCT(
    apple_receipt.environment,
    (
      SELECT AS STRUCT
        DATE(TIMESTAMP_MILLIS(original_purchase_date_ms)) AS start_date,
        DATE(TIMESTAMP_MILLIS(expires_date_ms)) AS end_date,
        TIMESTAMP_MILLIS(original_purchase_date_ms) AS start_time,
        TIMESTAMP_MILLIS(expires_date_ms) AS end_time,
        iap.derive_apple_subscription_interval(
          DATETIME(TIMESTAMP_MILLIS(purchase_date_ms), "America/Los_Angeles"),
          DATETIME(TIMESTAMP_MILLIS(expires_date_ms), "America/Los_Angeles")
        ).*,
      FROM
        UNNEST(apple_receipt.latest_receipt_info)
      WHERE
        is_trial_period = "false"
      ORDER BY
        expires_date_ms DESC
      LIMIT
        1
    ) AS active_period,
    (
      SELECT AS STRUCT
        TIMESTAMP_MILLIS(purchase_date_ms) AS start_time,
        TIMESTAMP_MILLIS(expires_date_ms) AS end_time
      FROM
        (
          SELECT
            purchase_date_ms,
            expires_date_ms
          FROM
            UNNEST(apple_receipt.latest_receipt_info)
          WHERE
            is_trial_period = "true"
          UNION ALL
          SELECT
            purchase_date_ms,
            expires_date_ms
          FROM
            UNNEST(apple_receipt.receipt.in_app)
          WHERE
            is_trial_period = "true"
        )
      ORDER BY
        expires_date_ms DESC
      LIMIT
        1
    ) AS trial_period
  )
);

SELECT
  assert.json_equals(
    STRUCT(
      "Production" AS environment,
      STRUCT(
        DATE "2020-01-01" AS start_date,
        DATE "2020-01-08" AS end_date,
        TIMESTAMP "2020-01-01" AS start_time,
        TIMESTAMP "2020-01-08" AS end_time,
        "day" AS `interval`,
        1 AS interval_count
      ) AS active_period,
      STRUCT(
        TIMESTAMP "2020-01-01" AS start_time,
        TIMESTAMP "2020-01-07" AS end_time
      ) AS trial_period
    ),
    iap.scrub_apple_receipt(
      STRUCT(
        "Production" AS environment,
        [
          STRUCT(
            UNIX_MILLIS(TIMESTAMP "2020-01-01") AS original_purchase_date_ms,
            UNIX_MILLIS(TIMESTAMP "2020-01-07") AS purchase_date_ms,
            UNIX_MILLIS(TIMESTAMP "2020-01-08") AS expires_date_ms,
            "false" AS is_trial_period
          )
        ] AS latest_receipt_info,
        STRUCT(
          [
            STRUCT(
              UNIX_MILLIS(TIMESTAMP "2020-01-01") AS purchase_date_ms,
              UNIX_MILLIS(TIMESTAMP "2020-01-07") AS expires_date_ms,
              "true" AS is_trial_period
            )
          ] AS in_app
        ) AS receipt
      )
    )
  )
