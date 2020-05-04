/*
Unnest and dedupe a nested array.

Returns a single array with deduped elements
from all nested arrays.
*/
CREATE OR REPLACE FUNCTION udf.unnest_dedupe_nested_array(arr ANY TYPE) AS (
  ARRAY(SELECT DISTINCT e FROM UNNEST(arr) AS nested_array, UNNEST(nested_array.arr) AS e)
);

-- Test
SELECT
  assert_array_equals(
    [1, 2, 3],
    udf.unnest_dedupe_nested_array(
      [STRUCT([1, 2, 1] AS arr), STRUCT([2, 2, 1] AS arr), STRUCT([3, 1, 3] AS arr)]
    )
  ),
  assert_array_equals([1], udf.unnest_dedupe_nested_array([STRUCT([1, 1, 1] AS arr)])),
  assert_array_equals([], udf.unnest_dedupe_nested_array([STRUCT([] AS arr)]));
