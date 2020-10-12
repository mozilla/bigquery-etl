/*

Return an array containing only distinct values of the given array

 */
CREATE OR REPLACE FUNCTION udf.dedupe_array(list ANY TYPE) AS (
  ARRAY(SELECT DISTINCT AS STRUCT * FROM UNNEST(list))
);

-- Test
SELECT
  assert.array_equals(['foo'], udf.dedupe_array(['foo'])),
  assert.array_equals(['foo'], udf.dedupe_array(['foo', 'foo'])),
  assert.array_equals(['foo', 'bar'], udf.dedupe_array(['foo', 'bar', 'bar', 'foo'])),
  assert.array_equals(
    ['foo', 'bar', 'baz'],
    udf.dedupe_array(['foo', 'bar', 'bar', 'baz', 'foo', 'baz', 'bar'])
  );
