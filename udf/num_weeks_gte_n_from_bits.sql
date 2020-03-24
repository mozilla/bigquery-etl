/*

Returns the number of weeks with >=n nonzero bits, given an integer representing
a bit array for the last 4 weeks.

*/
CREATE OR REPLACE FUNCTION udf.num_weeks_gte_n_from_bits(b BYTES, n INT64) AS (
  CAST(BIT_COUNT(b & udf.bitmask_range(1, 7)) >= n AS INT64) + CAST(
    BIT_COUNT(b & udf.bitmask_range(8, 7)) >= n AS INT64
  ) + CAST(BIT_COUNT(b & udf.bitmask_range(15, 7)) >= n AS INT64) + CAST(
    BIT_COUNT(b & udf.bitmask_range(22, 7)) >= n AS INT64
  )
);
