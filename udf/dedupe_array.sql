/*

Return an array containing only distinct values of the given array

 */
CREATE TEMP FUNCTION udf_dedupe_array(list ANY TYPE) AS (
  (
    SELECT
      ARRAY(
        SELECT AS STRUCT
          *
        FROM
          (
            SELECT
              DISTINCT *
            FROM
              UNNEST(list)
          )
      )
  )
);

-- Test

SELECT
  assert_array_equals(['foo'], udf_dedupe_array(['foo'])),
  assert_array_equals(['foo'], udf_dedupe_array(['foo', 'foo'])),
  assert_array_equals(['foo', 'bar'], udf_dedupe_array(['foo', 'bar', 'bar', 'foo'])),
  assert_array_equals(['foo', 'bar', 'baz'], udf_dedupe_array(['foo', 'bar', 'bar', 'baz', 'foo', 'baz', 'bar']));
