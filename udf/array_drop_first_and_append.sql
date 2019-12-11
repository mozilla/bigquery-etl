CREATE TEMP FUNCTION
  udf_array_drop_first_and_append(arr ANY TYPE, append ANY TYPE) AS (
    ARRAY_CONCAT(
      ARRAY(
        SELECT v
        FROM UNNEST(arr) AS v WITH OFFSET off
        WHERE off > 0
        ORDER BY off ASC),
      [append]));

-- 

SELECT
  assert_array_equals([2, 3, 4], udf_array_drop_first_and_append([1, 2, 3], 4)),
  assert_array_equals([4], udf_array_drop_first_and_append([], 4));
