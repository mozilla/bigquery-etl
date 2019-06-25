CREATE TEMP FUNCTION
  udf_shift_one_day(x INT64) AS (IFNULL((x << 1) & udf_bitmask_lowest_28(),
	0));
