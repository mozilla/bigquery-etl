/*

Returns true if the array arr contains the element el

*/
CREATE TEMP FUNCTION
  udf_legacy_contains(arr ANY TYPE, el ANY TYPE)
  RETURNS BOOLEAN
  AS (
    el IN UNNEST(arr)
  );

-- Tests

assert.true(udf_legacy_contains([1, 2, 3], 1)),
asset_false(udf_legacy_contains([1, 2, 3], 5))
