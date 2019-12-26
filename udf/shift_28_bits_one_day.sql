/*

Shift input bits one day left and drop any bits beyond 28 days.

*/

CREATE TEMP FUNCTION
  udf_shift_28_bits_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
    0));

-- Tests

SELECT
  assert_equals(2, udf_shift_28_bits_one_day(1)),
  assert_equals(1 << 8, udf_shift_28_bits_one_day(1 << 7)),
  assert_equals(1 << 27, udf_shift_28_bits_one_day(1 << 26)),
  assert_equals(0, udf_shift_28_bits_one_day(1 << 27));
