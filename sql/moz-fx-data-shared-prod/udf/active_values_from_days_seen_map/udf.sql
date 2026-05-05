/*
Given a map of representing activity for STRING `key`s, this
function returns an array of which `key`s were active for the
time period in question.

start_offset should be at most 0.
n_bits should be at most the remaining bits.
*/
CREATE OR REPLACE FUNCTION udf.active_values_from_days_seen_map(
  days_seen_bits_map ARRAY<STRUCT<key STRING, value INT64>>,
  start_offset INT64,
  n_bits INT64
) AS (
  ARRAY(
    SELECT DISTINCT
      key
    FROM
      UNNEST(days_seen_bits_map)
    WHERE
      udf.bits28_active_in_range(value, start_offset, n_bits)
  )
);

-- Tests
SELECT
  mozfun.assert.array_equals(
    ['a', 'b'],
    udf.active_values_from_days_seen_map(
      [STRUCT('a' AS key, 1 AS value), STRUCT('b' AS key, 3 AS value)],
      0,
      1
    )
  ),
  mozfun.assert.array_equals(
    ['a'],
    udf.active_values_from_days_seen_map(
      [STRUCT('a' AS key, 2048 AS value), STRUCT('b' AS key, 3 AS value)],
      -14,
      7
    )
  ),
  mozfun.assert.array_equals(
    ['b'],
    udf.active_values_from_days_seen_map(
      [STRUCT('a' AS key, 2048 AS value), STRUCT('b' AS key, 3 AS value)],
      -6,
      7
    )
  ),
  mozfun.assert.array_equals(
    ['a', 'b'],
    udf.active_values_from_days_seen_map(
      [STRUCT('a' AS key, 1 AS value), STRUCT('b' AS key, 3 AS value)],
      -27,
      28
    )
  );
