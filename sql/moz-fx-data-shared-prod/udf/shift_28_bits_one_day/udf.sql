/*

Shift input bits one day left and drop any bits beyond 28 days.

*/
CREATE OR REPLACE FUNCTION udf.shift_28_bits_one_day(x INT64) AS (
  IFNULL((x << 1) & udf.bitmask_lowest_28(), 0)
);

-- Tests
SELECT
  assert.equals(2, udf.shift_28_bits_one_day(1)),
  assert.equals(1 << 8, udf.shift_28_bits_one_day(1 << 7)),
  assert.equals(1 << 27, udf.shift_28_bits_one_day(1 << 26)),
  assert.equals(0, udf.shift_28_bits_one_day(1 << 27));
