/*
Drop the first element of an array, and append the given element.

Result is an array with the same length as the input.
*/
CREATE OR REPLACE FUNCTION udf.array_drop_first_and_append(arr ANY TYPE, append ANY TYPE) AS (
  ARRAY_CONCAT(
    ARRAY(SELECT v FROM UNNEST(arr) AS v WITH OFFSET off WHERE off > 0 ORDER BY off ASC),
    [append]
  )
);

-- Tests
SELECT
  mozfun.assert.array_equals([2, 3, 4], udf.array_drop_first_and_append([1, 2, 3], 4)),
  mozfun.assert.array_equals([4], udf.array_drop_first_and_append([], 4));
