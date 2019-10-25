CREATE TEMP FUNCTION
  udf_array_sum(a ARRAY<INT64>) AS ((
    SELECT SUM(v)
    FROM UNNEST(a) v));

-- 

SELECT
  assert_equals(6, udf_array_sum(ARRAY [1, 2, 3])),
  assert_equals(6, udf_array_sum(ARRAY [1, 2, 3, NULL])),
  assert_equals(6, udf_array_sum(ARRAY [1, 2, 3, 0])),
  assert_equals(NULL, udf_array_sum(NULL));
