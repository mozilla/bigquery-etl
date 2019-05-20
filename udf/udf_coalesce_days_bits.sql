CREATE TEMP FUNCTION
  udf_bitmask_lowest_28() AS (0x0FFFFFFF);
  --
CREATE TEMP FUNCTION
  udf_shift_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
      0));
  --
CREATE TEMP FUNCTION
  udf_coalesce_adjacent_days_bits(prev INT64,
    curr INT64) AS ( COALESCE( NULLIF(udf_shift_one_day(prev),
        0),
      curr,
      0));

/*

We generally want to believe only the first reasonable profile creation
date that we receive from a client.
Given bits representing usage from the previous day and the current day,
this function shifts the first argument by one day and returns either that
value if non-zero and non-null, the current day value if non-zero and non-null,
or else 0.

Example:

SELECT
  udf_coalesce_adjacent_days_bits(1,
    64),
  udf_coalesce_adjacent_days_bits(64,
    1),
  udf_coalesce_adjacent_days_bits(0,
    64),
  udf_coalesce_adjacent_days_bits(NULL,
    64),
  udf_coalesce_adjacent_days_bits(NULL,
    NULL);
2, 128, 64, 64, 0

*/

