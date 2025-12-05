CREATE OR REPLACE FUNCTION utils.timestamp_diff_complete_weeks(
  end_timestamp TIMESTAMP,
  start_timestamp TIMESTAMP
)
RETURNS INTEGER AS (
  DIV(TIMESTAMP_DIFF(end_timestamp, start_timestamp, DAY), 7)
);

-- Tests
SELECT
  mozfun.assert.equals(
    expected,
    utils.timestamp_diff_complete_weeks(end_timestamp, start_timestamp)
  ),
  mozfun.assert.equals(
    -expected,
    utils.timestamp_diff_complete_weeks(start_timestamp, end_timestamp)
  )
FROM
  UNNEST(
    ARRAY<STRUCT<start_timestamp TIMESTAMP, end_timestamp TIMESTAMP, expected INTEGER>>[
      (TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-01-01 00:00:00', 0),
      (TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-01-07 23:59:59.999999', 0),
      (TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-01-08 00:00:00', 1),
      (TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-01-08 00:00:00', 0),
      (TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-01-08 12:00:00', 1)
    ]
  )
