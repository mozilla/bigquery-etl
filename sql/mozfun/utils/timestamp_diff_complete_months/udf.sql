CREATE OR REPLACE FUNCTION utils.timestamp_diff_complete_months(
  end_timestamp TIMESTAMP,
  start_timestamp TIMESTAMP
)
RETURNS INTEGER AS (
  (
    -- Start with the number of month boundaries between the two timestamps.
    DATE_DIFF(
      DATE(GREATEST(start_timestamp, end_timestamp)),
      DATE(LEAST(start_timestamp, end_timestamp)),
      MONTH
    )
    -- If the later timestamp isn't as far along in its month as the earlier timestamp is in its month
    -- then there's one less complete month.
    - IF(
      (
        TIMESTAMP_DIFF(
          GREATEST(start_timestamp, end_timestamp),
          TIMESTAMP_TRUNC(GREATEST(start_timestamp, end_timestamp), MONTH),
          MICROSECOND
        ) < TIMESTAMP_DIFF(
          LEAST(start_timestamp, end_timestamp),
          TIMESTAMP_TRUNC(LEAST(start_timestamp, end_timestamp), MONTH),
          MICROSECOND
        )
      ),
      1,
      0
    )
  )
  -- If `end_timestamp` is earlier than `start_timestamp` then the output is negative.
  * IF(end_timestamp < start_timestamp, -1, 1)
);

-- Tests
SELECT
  mozfun.assert.equals(
    expected,
    utils.timestamp_diff_complete_months(end_timestamp, start_timestamp)
  ),
  mozfun.assert.equals(
    -expected,
    utils.timestamp_diff_complete_months(start_timestamp, end_timestamp)
  )
FROM
  UNNEST(
    ARRAY<STRUCT<start_timestamp TIMESTAMP, end_timestamp TIMESTAMP, expected INTEGER>>[
      (TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-01-01 00:00:00', 0),
      (TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-01-31 23:59:59.999999', 0),
      (TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-02-01 00:00:00', 1),
      (TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-02-01 00:00:00', 0),
      (TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-02-01 12:00:00', 1),
      (TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-03-01 00:00:00', 1),
      (TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-03-01 12:00:00', 2),
      (TIMESTAMP '2025-01-31 00:00:00', TIMESTAMP '2025-02-28 23:59:59.999999', 0),
      (TIMESTAMP '2025-01-31 00:00:00', TIMESTAMP '2025-03-01 00:00:00', 1),
      (TIMESTAMP '2025-01-31 00:00:00', TIMESTAMP '2025-03-31 00:00:00', 2),
      (TIMESTAMP '2025-01-31 12:00:00', TIMESTAMP '2025-03-31 00:00:00', 1),
      (TIMESTAMP '2025-01-31 12:00:00', TIMESTAMP '2025-03-31 12:00:00', 2),
      (TIMESTAMP '2025-02-28 00:00:00', TIMESTAMP '2025-03-28 00:00:00', 1),
      (TIMESTAMP '2025-02-28 12:00:00', TIMESTAMP '2025-03-28 00:00:00', 0),
      (TIMESTAMP '2025-02-28 12:00:00', TIMESTAMP '2025-03-28 12:00:00', 1),
      (TIMESTAMP '2025-02-28 12:00:00', TIMESTAMP '2025-03-29 00:00:00', 1),
      (TIMESTAMP '2025-02-28 12:00:00', TIMESTAMP '2025-04-01 00:00:00', 1)
    ]
  )
