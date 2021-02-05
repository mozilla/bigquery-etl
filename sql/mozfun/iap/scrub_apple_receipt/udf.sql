CREATE OR REPLACE FUNCTION iap.scrub_apple_receipt(apple_receipt ANY TYPE)
RETURNS STRUCT<
  environment STRING,
  active_periods ARRAY<STRUCT<start_date DATE, end_date DATE>>
> AS (
  STRUCT(
    apple_receipt.environment,
    -- extract a list of start and end dates that doesn't overlap or repeat
    ARRAY(
      WITH exploded AS (
        SELECT DISTINCT
          date,
          IF(
            -- TRUE when date - 1 is missing
            ANY_VALUE(date) OVER (
              ORDER BY
                UNIX_DATE(date)
              RANGE BETWEEN
                1 PRECEDING
                AND 1 PRECEDING
            ) IS NULL,
            date,
            NULL
          ) AS start_date,
          IF(
            -- TRUE when date + 1 is missing
            ANY_VALUE(date) OVER (
              ORDER BY
                UNIX_DATE(date)
              RANGE BETWEEN
                1 FOLLOWING
                AND 1 FOLLOWING
            ) IS NULL,
            date,
            NULL
          ) AS end_date,
        FROM
          UNNEST(apple_receipt.receipt.in_app)
        CROSS JOIN
          UNNEST(
            GENERATE_DATE_ARRAY(
              DATE(TIMESTAMP_MILLIS(SAFE_CAST(purchase_date_ms AS INT64))),
              DATE(TIMESTAMP_MILLIS(SAFE_CAST(expires_date_ms AS INT64)))
            )
          ) AS date
        WHERE
          is_trial_period = "false"
      ),
      matched AS (
        SELECT
          start_date,
          -- MIN(end_date) where end_date >= start_date
          MIN(end_date) OVER (
            ORDER BY
              date
            ROWS BETWEEN
              CURRENT ROW
              AND UNBOUNDED FOLLOWING
          ) AS end_date,
        FROM
          exploded
      )
      SELECT AS STRUCT
        *
      FROM
        matched
      WHERE
        start_date IS NOT NULL
    ) AS active_periods
  )
);

SELECT
  assert.json_equals(
    STRUCT(
      "Production" AS environment,
      [
        STRUCT(DATE "2020-01-01" AS start_date, "2020-01-03" AS end_date),
        ("2020-01-05", "2020-01-05"),
        ("2020-01-07", "2020-01-08")
      ] AS active_periods
    ),
    iap.scrub_apple_receipt(
      STRUCT(
        "Production" AS environment,
        STRUCT(
          [
            STRUCT(
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-01") AS STRING) AS purchase_date_ms,
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-01T8:00:00") AS STRING) AS expires_date_ms,
              "false" AS is_trial_period
            ),
            (
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-01T12:00:00") AS STRING),
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-01T20:00:00") AS STRING),
              "false"
            ),
            (
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-02") AS STRING),
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-03") AS STRING),
              "false"
            ),
            (
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-05") AS STRING),
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-05") AS STRING),
              "false"
            ),
            (
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-07") AS STRING),
              CAST(UNIX_MILLIS(TIMESTAMP "2020-01-08") AS STRING),
              "false"
            )
          ] AS in_app
        ) AS receipt
      )
    )
  )
