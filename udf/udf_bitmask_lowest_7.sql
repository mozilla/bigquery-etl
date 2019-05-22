-- Equivalent to, but more efficient than, udf_bitmask_range(1,7)
CREATE TEMP FUNCTION
  udf_bitmask_lowest_7() AS (0x7F);
