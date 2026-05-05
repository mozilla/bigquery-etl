/*
Returns the bitfield shifted by one day, 0 for NULL
*/
CREATE OR REPLACE FUNCTION udf.shift_one_day(x INT64) AS (
  IFNULL((x << 1) & udf.bitmask_lowest_28(), 0)
);

-- Tests
SELECT
  mozfun.assert.equals(4, udf.shift_one_day(2)),
  mozfun.assert.equals(0, udf.shift_one_day(NULL))
