CREATE OR REPLACE FUNCTION iap.derive_apple_subscription_interval(start DATETIME, `end` DATETIME)
RETURNS STRUCT<`interval` STRING, interval_count INT64> AS (
  (
    WITH interval_counts AS (
      SELECT
        DATETIME_DIFF(`end`, start, YEAR) AS years,
        DATETIME_DIFF(`end`, start, MONTH) AS months,
        DATETIME_DIFF(`end`, start, WEEK) AS weeks,
        DATETIME_DIFF(`end`, start, DAY) AS days,
    )
    SELECT
      CASE
        WHEN DATETIME_ADD(start, INTERVAL years YEAR) = `end`
          THEN ("year", years)
        WHEN DATETIME_ADD(start, INTERVAL months MONTH) = `end`
          OR DATETIME_SUB(`end`, INTERVAL months MONTH) = start
          THEN ("month", months)
        WHEN DATETIME_ADD(start, INTERVAL weeks WEEK) = `end`
          THEN ("week", weeks)
        ELSE ("day", days)
      END
    FROM
      interval_counts
  )
);

SELECT
  assert.equals(expected, iap.derive_apple_subscription_interval(start, `end`))
FROM
  UNNEST(
    ARRAY<
      STRUCT<
        expected STRUCT<`interval` STRING, interval_count INT64>,
        start DATETIME,
        `end` DATETIME
      >
    >[
      (("year", 1), DATETIME "2020-03-15 00:00:00", DATETIME "2021-03-15 00:00:00"),
      (("month", 6), DATETIME "2021-03-15 00:00:00", DATETIME "2021-09-15 00:00:00"),
      (("month", 3), DATETIME "2021-03-15 00:00:00", DATETIME "2021-06-15 00:00:00"),
      (("month", 1), DATETIME "2021-03-15 00:00:00", DATETIME "2021-04-15 00:00:00"),
      (("week", 2), DATETIME "2021-03-15 00:00:00", DATETIME "2021-03-29 00:00:00"),
      (("week", 1), DATETIME "2021-03-15 00:00:00", DATETIME "2021-03-22 00:00:00"),
      (("day", 2), DATETIME "2021-03-15 00:00:00", DATETIME "2021-03-17 00:00:00"),
      (("day", 1), DATETIME "2021-03-15 00:00:00", DATETIME "2021-03-16 00:00:00"),
      -- last day of month
      (("month", 1), DATETIME "2021-02-28 00:00:00", DATETIME "2021-03-31 00:00:00"),
      (("month", 1), DATETIME "2021-01-31 00:00:00", DATETIME "2021-02-28 00:00:00"),
      (("month", 1), DATETIME "2021-02-28 00:00:00", DATETIME "2021-03-28 00:00:00"),
      (("month", 1), DATETIME "2021-01-28 00:00:00", DATETIME "2021-02-28 00:00:00"),
      -- daylight savings time
      (
        ("month", 6),
        DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
        DATETIME(TIMESTAMP "2021-09-14 00:00:00 UTC", "America/Los_Angeles")
      ),
      (
        ("month", 3),
        DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
        DATETIME(TIMESTAMP "2021-06-14 00:00:00 UTC", "America/Los_Angeles")
      ),
      (
        ("month", 1),
        DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
        DATETIME(TIMESTAMP "2021-04-14 00:00:00 UTC", "America/Los_Angeles")
      ),
      (
        ("week", 1),
        DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
        DATETIME(TIMESTAMP "2021-03-21 00:00:00 UTC", "America/Los_Angeles")
      ),
      (
        ("day", 1),
        DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
        DATETIME(TIMESTAMP "2021-03-15 00:00:00 UTC", "America/Los_Angeles")
      ),
      (
        ("day", 2),
        DATETIME(TIMESTAMP "2021-03-14 01:00:00 UTC", "America/Los_Angeles"),
        DATETIME(TIMESTAMP "2021-03-16 00:00:00 UTC", "America/Los_Angeles")
      ),
      -- february PST vs march UTC
      (
        ("month", 1),
        DATETIME(TIMESTAMP "2021-03-01 01:00:00 UTC", "America/Los_Angeles"),
        DATETIME(TIMESTAMP "2021-03-29 00:00:00 UTC", "America/Los_Angeles")
      )
    ]
  )
