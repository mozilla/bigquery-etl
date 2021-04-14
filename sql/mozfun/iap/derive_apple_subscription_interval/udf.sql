CREATE OR REPLACE FUNCTION iap.derive_apple_subscription_interval(
  purchase_datetime_pst DATETIME,
  expires_datetime_pst DATETIME
) AS (
  CASE
  WHEN
    DATETIME_ADD(purchase_datetime_pst, INTERVAL 1 YEAR) = expires_datetime_pst
  THEN
    "year"
  WHEN
    DATETIME_ADD(purchase_datetime_pst, INTERVAL 1 QUARTER) = expires_datetime_pst
  THEN
    "quarter"
  WHEN
    DATETIME_ADD(purchase_datetime_pst, INTERVAL 1 MONTH) = expires_datetime_pst
  THEN
    "month"
  WHEN
    DATETIME_ADD(purchase_datetime_pst, INTERVAL 1 WEEK) = expires_datetime_pst
  THEN
    "week"
  WHEN
    DATETIME_ADD(purchase_datetime_pst, INTERVAL 1 DAY) = expires_datetime_pst
  THEN
    "day"
  ELSE
    CONCAT(CAST(DATETIME_DIFF(expires_datetime_pst, purchase_datetime_pst, DAY) AS STRING), " day")
  END
);

SELECT
  assert.equals(
    "year",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2020-03-15 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-15 01:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "quarter",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-15 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-06-15 01:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "month",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-15 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-04-15 01:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "week",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-15 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-22 01:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "day",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-15 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-16 01:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "2 day",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-15 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-17 01:00:00 UTC", "America/Los_Angeles")
    )
  ),
  -- daylight savings time
  assert.equals(
    "quarter",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-06-14 00:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "month",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-04-14 00:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "week",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-21 00:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "day",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-15 00:00:00 UTC", "America/Los_Angeles")
    )
  ),
  assert.equals(
    "2 day",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-16 00:00:00 UTC", "America/Los_Angeles")
    )
  ),
  -- february vs march PST
  assert.equals(
    "month",
    iap.derive_apple_subscription_interval(
      DATETIME(TIMESTAMP "2021-03-01 01:00:00 UTC", "America/Los_Angeles"),
      DATETIME(TIMESTAMP "2021-03-29 00:00:00 UTC", "America/Los_Angeles")
    )
  ),
