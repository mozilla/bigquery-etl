-- Equivalent to, but more efficient than, udf_bitmask_range(1,28)
CREATE TEMP FUNCTION
  udf_bitmask_lowest_28() AS (0x0FFFFFFF);
