CREATE OR REPLACE FUNCTION vpn.pricing_plan(
  provider STRING,
  amount INTEGER,
  currency STRING,
  `interval` STRING,
  interval_count INTEGER
)
RETURNS STRING AS (
  IF(
    provider = "Apple Store",
    -- currency and amount are not available for apple plans, so use this format instead
    CONCAT(interval_count, "-", `interval`, "-", "apple"),
    CONCAT(interval_count, "-", `interval`, "-", currency, "-", (amount / 100))
  )
);

-- Tests
SELECT
  assert.equals(
    "1-month-apple",
    vpn.pricing_plan(
      provider => "Apple Store",
      amount => NULL,
      currency => NULL,
      `interval` => "month",
      interval_count => 1
    )
  ),
  assert.equals(
    "1-year-usd-59.99",
    vpn.pricing_plan(
      provider => "Stripe",
      amount => 5999,
      currency => "usd",
      `interval` => "year",
      interval_count => 1
    )
  ),
