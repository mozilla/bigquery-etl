/*
This function counts the 1s in lowest 7 bits of an INT64
*/
CREATE OR REPLACE FUNCTION udf.bitcount_lowest_7(x INT64) AS (
  BIT_COUNT(x & udf.bitmask_lowest_7())
);

-- Tests
SELECT
  mozfun.assert.equals(7, udf.bitcount_lowest_7(255)),
  mozfun.assert.equals(1, udf.bitcount_lowest_7(2))
