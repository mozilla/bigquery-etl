/*

Combines two bit patterns. The first pattern represents activity over a 28-day
period ending "yesterday". The second pattern represents activity as observed
today (usually just 0 or 1). We shift the bits in the first pattern by one to
set the new baseline as "today", then perform a bitwise OR of the two patterns.

*/
CREATE OR REPLACE FUNCTION udf.combine_adjacent_days_28_bits(prev INT64, curr INT64) AS (
  udf.shift_28_bits_one_day(prev) | IFNULL(curr, 0)
);

-- Tests
SELECT
  mozfun.assert.equals(3, udf.combine_adjacent_days_28_bits(1, 1)),
  mozfun.assert.equals(4, udf.combine_adjacent_days_28_bits(2, 4)),
  mozfun.assert.equals(6, udf.combine_adjacent_days_28_bits(3, 4)),
  mozfun.assert.equals(4, udf.combine_adjacent_days_28_bits(2, NULL));
