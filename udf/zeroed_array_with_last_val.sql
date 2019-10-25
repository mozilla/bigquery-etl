CREATE TEMP FUNCTION
  udf_zeroed_array_with_last_val(val INT64)  AS (
    ARRAY [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, val]);
