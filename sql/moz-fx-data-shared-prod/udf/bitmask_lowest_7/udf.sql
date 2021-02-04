-- Equivalent to, but more efficient than, udf.bitmask_range(1,7)
CREATE OR REPLACE FUNCTION udf.bitmask_lowest_7() AS (
  0x7F
);
