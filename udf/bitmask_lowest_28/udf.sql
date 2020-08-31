-- Equivalent to, but more efficient than, udf.bitmask_range(1,28)
CREATE OR REPLACE FUNCTION udf.bitmask_lowest_28() AS (
  0x0FFFFFFF
);
