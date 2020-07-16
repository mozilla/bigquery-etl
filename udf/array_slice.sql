-- Return subset of array between start_index and end_index (inclusive)
-- This function can not be used on arrays of structs
CREATE OR REPLACE FUNCTION udf.array_slice(arr ANY TYPE, start_index INT64, end_index INT64) AS (
  ARRAY(
    SELECT
      * EXCEPT (offset)
    FROM
      UNNEST(arr)
      WITH OFFSET
    WHERE
      offset
      BETWEEN start_index
      AND end_index
    ORDER BY
      offset
  )
);

-- Test
SELECT
  assert_array_equals([1, 2, 3], udf.array_slice([1, 2, 3], 0, 2)),
  assert_array_equals([1, 2], udf.array_slice([1, 2, 3], 0, 1)),
  assert_array_equals(['2'], udf.array_slice(['1', '2', '3'], 1, 1)),
