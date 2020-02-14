CREATE OR REPLACE FUNCTION 
  udf.bitcount_lowest_7(x INT64) AS (
  	BIT_COUNT(x & udf.bitmask_lowest_7())
  );
