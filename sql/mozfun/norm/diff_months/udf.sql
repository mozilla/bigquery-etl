CREATE OR REPLACE FUNCTION norm.diff_months(
  start DATETIME,
  `end` DATETIME,
  grace_period INTERVAL,
  inclusive BOOLEAN
) AS (
  (
    SELECT
      IFNULL(MAX(months), 0)
    FROM
      UNNEST([DATETIME_DIFF(`end`, start, MONTH)]) AS diff,
      UNNEST(GENERATE_ARRAY(diff - 2, diff)) AS months
    WHERE
      -- return 0 via IFNULL
      months > 0
      -- don't count partial months
      AND IF(
        inclusive,
        -- compare in both directions to handle end of month correctly
        start + INTERVAL months MONTH + grace_period <= `end`
        AND start <= `end` - INTERVAL months MONTH - grace_period,
        -- exclusive
        start + INTERVAL months MONTH + grace_period < `end`
        AND start < `end` - INTERVAL months MONTH - grace_period
      )
  )
);

-- Tests
SELECT
  assert.equals(
    expected,
    norm.diff_months(start, `end`, grace_period => INTERVAL 2 DAY, inclusive => FALSE)
  )
FROM
  UNNEST(
    ARRAY<STRUCT<start DATETIME, `end` DATETIME, expected INT64>>[
      -- start at middle of month
      (DATETIME "2021-01-15", DATETIME "2021-03-18", 2),
      (DATETIME "2021-01-15", DATETIME "2021-03-17", 1),
      (DATETIME "2021-01-15", DATETIME "2021-03-16", 1),
      (DATETIME "2021-01-15", DATETIME "2021-03-15", 1),
      (DATETIME "2021-01-15", DATETIME "2021-02-18", 1),
      (DATETIME "2021-01-15", DATETIME "2021-02-17", 0),
      (DATETIME "2021-01-15", DATETIME "2021-02-16", 0),
      (DATETIME "2021-01-15", DATETIME "2021-02-15", 0),
      -- grace period does not cause negative result
      (DATETIME "2021-01-15", DATETIME "2021-01-18", 0),
      (DATETIME "2021-01-15", DATETIME "2021-01-17", 0),
      (DATETIME "2021-01-15", DATETIME "2021-01-16", 0),
      -- start at end of short month
      (DATETIME "2021-02-28", DATETIME "2021-06-03", 3),
      (DATETIME "2021-02-28", DATETIME "2021-06-02", 2),
      (DATETIME "2021-02-28", DATETIME "2021-06-01", 2),
      (DATETIME "2021-02-28", DATETIME "2021-05-31", 2),
      (DATETIME "2021-02-28", DATETIME "2021-05-30", 2),
      (DATETIME "2021-02-28", DATETIME "2021-05-29", 2),
      (DATETIME "2021-02-28", DATETIME "2021-05-28", 2),
      -- start at end of long month
      (DATETIME "2021-01-31", DATETIME "2021-05-03", 3),
      (DATETIME "2021-01-31", DATETIME "2021-05-02", 2),
      (DATETIME "2021-01-31", DATETIME "2021-05-01", 2),
      (DATETIME "2021-01-31", DATETIME "2021-04-30", 2),
      -- end near end of short month
      (DATETIME "2021-01-30", DATETIME "2021-04-02", 2),
      (DATETIME "2021-01-30", DATETIME "2021-04-01", 1),
      (DATETIME "2021-01-30", DATETIME "2021-03-31", 1),
      (DATETIME "2021-01-30", DATETIME "2021-03-30", 1),
      (DATETIME "2021-01-30", DATETIME "2021-03-03", 1),
      (DATETIME "2021-01-30", DATETIME "2021-03-02", 0),
      -- end near end of shortest month
      (DATETIME "2021-01-28", DATETIME "2021-03-31", 2),
      (DATETIME "2021-01-28", DATETIME "2021-03-30", 1),
      (DATETIME "2021-01-28", DATETIME "2021-03-03", 1),
      (DATETIME "2021-01-28", DATETIME "2021-03-02", 0),
      (DATETIME "2021-01-28", DATETIME "2021-03-01", 0),
      (DATETIME "2021-01-28", DATETIME "2021-02-28", 0)
    ]
  )
