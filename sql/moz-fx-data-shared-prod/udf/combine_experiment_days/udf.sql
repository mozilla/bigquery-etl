/*

The "clients_last_seen" class of tables represent various types of client
activity within a 28-day window as bit patterns.

This function takes in two arrays of structs where each entry gives the
bit pattern for days in which we saw a ping for a given user in a given
experiment. We combine the bit patterns for the previous day and the
current day, returning a single array of experiment structs.

*/
CREATE OR REPLACE FUNCTION udf.combine_experiment_days(
    --
  prev ARRAY<STRUCT<experiment STRING, branch STRING, bits INT64>>,
    --
  curr ARRAY<STRUCT<experiment STRING, branch STRING, bits INT64>>
) AS (
    -- The below is logically a FULL JOIN, but BigQuery returns error
    -- "Array scan is not allowed with FULL JOIN" so we have to do two
    -- separate scans.
  ARRAY_CONCAT(
      -- Experiments present in prev (and potentially in curr too)
    ARRAY(
      SELECT AS STRUCT
        experiment,
        branch,
        udf.combine_adjacent_days_28_bits(prev.bits, curr.bits) AS bits
      FROM
        UNNEST(prev) AS prev
      LEFT JOIN
        UNNEST(curr) AS curr
        USING (experiment, branch)
      WHERE
        udf.combine_adjacent_days_28_bits(prev.bits, curr.bits) > 0
    ),
      -- Experiments present in curr only
    ARRAY(
      SELECT AS STRUCT
        experiment,
        branch,
        curr.bits AS bits
      FROM
        UNNEST(curr) AS curr
      LEFT JOIN
        UNNEST(prev) AS prev
        USING (experiment, branch)
      WHERE
        prev IS NULL
    )
  )
);

-- Tests
SELECT
  mozfun.assert.array_equals(
    udf.combine_experiment_days(
      [
        STRUCT("exp1" AS experiment, "1a" AS branch, 1 AS bits),
        STRUCT("exp2" AS experiment, "2b" AS branch, 3 AS bits),
        STRUCT("filtered_out" AS experiment, "?" AS branch, 1 << 27 AS bits)
      ],
      [
        STRUCT("exp1" AS experiment, "1a" AS branch, 1 AS bits),
        STRUCT("exp3" AS experiment, "3c" AS branch, 1 AS bits)
      ]
    ),
    [
      STRUCT("exp1" AS experiment, "1a" AS branch, 3 AS bits),
      STRUCT("exp2" AS experiment, "2b" AS branch, 6 AS bits),
      STRUCT("exp3" AS experiment, "3c" AS branch, 1 AS bits)
    ]
  );
