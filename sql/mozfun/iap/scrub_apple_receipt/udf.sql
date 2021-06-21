CREATE OR REPLACE FUNCTION iap.scrub_apple_receipt(apple_receipt ANY TYPE)
RETURNS STRUCT<
  environment STRING,
  active_periods ARRAY<
    STRUCT<
      -- *_date fields deprecated in favor of TIMESTAMP fields
      start_date DATE,
      end_date DATE,
      start_time TIMESTAMP,
      end_time TIMESTAMP,
      `interval` STRING,
      interval_count INT64
    >
  >
> AS (
  STRUCT(
    apple_receipt.environment,
    ARRAY(
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
    ) AS active_periods
  )
);

SELECT
  assert.json_equals(
    STRUCT(
      "Production" AS environment,
      [
        STRUCT(
          DATE "2020-01-01" AS start_date,
          DATE "2020-01-08" AS end_date,
          TIMESTAMP "2020-01-01" AS start_time,
          TIMESTAMP "2020-01-08" AS end_time,
          "day" AS `interval`,
          1 AS interval_count
        )
      ] AS active_periods
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
        ] AS latest_receipt_info
      )
    )
  )
