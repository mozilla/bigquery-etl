  -- Equivalent to, but more efficient than, calling udf_bitmask_range(1, 28)
CREATE TEMP FUNCTION
  udf_bitmask_lowest_28() AS (0x0FFFFFFF);
  --
CREATE TEMP FUNCTION
  udf_shift_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
      0));
  --
CREATE TEMP FUNCTION
  udf_combine_days(prev INT64,
    curr INT64) AS (udf_shift_one_day(prev) + IFNULL(curr,
      0));
  --
CREATE TEMP FUNCTION
  udf_combine_experiment_days(
    --
    prev ARRAY<STRUCT<experiment STRING,
    branch STRING,
    bits INT64>>,
    --
    curr ARRAY<STRUCT<experiment STRING,
    branch STRING,
    bits INT64>>) AS (
    -- The below is logically a FULL JOIN, but BigQuery returns error
    -- "Array scan is not allowed with FULL JOIN" so we have to do two
    -- separate scans.
    ARRAY_CONCAT(
      -- Experiments present in prev (and potentially in curr too)
      ARRAY(
      SELECT
        AS STRUCT experiment,
        branch,
        udf_combine_days(prev.bits,
          curr.bits) AS bits
      FROM
        UNNEST(prev) AS prev
      LEFT JOIN
        UNNEST(curr) AS curr
      USING
        (experiment,
          branch)
      WHERE
        udf_combine_days(prev.bits,
          curr.bits) > 0),
      -- Experiments present in curr only
      ARRAY(
      SELECT
        AS STRUCT experiment,
        branch,
        curr.bits AS bits
      FROM
        UNNEST(curr) AS curr
      LEFT JOIN
        UNNEST(prev) AS prev
      USING
        (experiment,
          branch)
      WHERE
        prev IS NULL)));

/*

The "clients_last_seen" class of tables represent various types of client
activity within a 28-day window as bit patterns.

This function takes in two arrays of structs where each entry gives the
bit pattern for days in which we saw a ping for a given user in a given
experiment. We combine the bit patterns for the previous day and the
current day, returning a single array of experiment structs.

SELECT udf_combine_experiment_days(
  [STRUCT("exp1" AS experiment, "1a" AS branch, 1 AS bits),
   STRUCT("exp2" AS experiment, "2b" AS branch, 3 AS bits),
   STRUCT("filtered_out" AS experiment, "?" AS branch, 1 << 27 AS bits)],
  [STRUCT("exp1" AS experiment, "1a" AS branch, 1 AS bits),
   STRUCT("exp3" AS experiment, "3c" AS branch, 1 AS bits)]);
exp1 1a 3
exp2 2b 6
exp3 3c 1

*/
