/*
The "clients_last_seen" class of tables represent various types of client
activity within a 28-day window as bit patterns.

This function takes in two arrays of structs (aka maps) where each entry gives the
bit pattern for days in which we saw a ping for a given user in a given
key. We combine the bit patterns for the previous day and the
current day, returning a single map.

See `udf.combine_experiment_days` for a more specific example of this approach.
*/
CREATE OR REPLACE FUNCTION udf.combine_days_seen_maps(
    --
  prev ARRAY<STRUCT<key STRING, value INT64>>,
    --
  curr ARRAY<STRUCT<key STRING, value INT64>>
) AS (
    -- The below is logically a FULL JOIN, but BigQuery returns error
    -- "Array scan is not allowed with FULL JOIN" so we have to do two
    -- separate scans.
  ARRAY_CONCAT(
      -- Keys present in prev (and potentially in curr too)
    ARRAY(
      SELECT AS STRUCT
        key,
        udf.combine_adjacent_days_28_bits(prev.value, curr.value) AS value
      FROM
        UNNEST(prev) AS prev
      LEFT JOIN
        UNNEST(curr) AS curr
      USING
        (key)
      WHERE
        udf.combine_adjacent_days_28_bits(prev.value, curr.value) > 0
    ),
      -- Keys present in curr only
    ARRAY(
      SELECT AS STRUCT
        key,
        curr.value
      FROM
        UNNEST(curr) AS curr
      LEFT JOIN
        UNNEST(prev) AS prev
      USING
        (key)
      WHERE
        prev IS NULL
    )
  )
);

-- Tests
SELECT
  mozfun.assert.array_equals(
    [
      STRUCT("key1" AS key, 3 AS value),
      STRUCT("key2" AS key, 6 AS value),
      STRUCT("key3" AS key, 1 AS value)
    ],
    udf.combine_days_seen_maps(
      [
        STRUCT("key1" AS key, 1 AS value),
        STRUCT("key2" AS key, 3 AS value),
        STRUCT("key3" AS key, 1 << 27 AS value)
      ],
      [STRUCT("key1" AS key, 1 AS value), STRUCT("key3" AS key, 1 AS value)]
    )
  );
