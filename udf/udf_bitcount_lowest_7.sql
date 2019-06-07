CREATE TEMP FUNCTION 
  udf_bitcount_lowest_7(x INT64) AS (
  	BIT_COUNT(x & udf_bitmask_lowest_7())
  );
