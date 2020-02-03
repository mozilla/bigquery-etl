CREATE OR REPLACE FUNCTION
  udf.shift_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
	0));
