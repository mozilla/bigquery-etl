/*

Returns the most frequently occuring element in an array.

In the case of multiple values tied for the highest count, it returns the value
that appears latest in the array. Nulls are retained.

See also: `mode.last, which ignores nulls.

*/
CREATE OR REPLACE FUNCTION mozfun.mode.last_retain_nulls(list ANY TYPE) AS (
  (
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
      WITH OFFSET AS _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(*) DESC,
      MAX(_offset) DESC
    LIMIT
      1
  )
);

-- Test
SELECT
  assert_equals('bar', mozfun.mode.last_retain_nulls(['foo', 'bar', 'baz', 'bar', 'fred'])),
  assert_equals('baz', mozfun.mode.last_retain_nulls(['foo', 'bar', 'baz', 'bar', 'baz', 'fred'])),
  assert_equals(CAST(NULL AS STRING), mozfun.mode.last_retain_nulls([NULL, 'foo', NULL]));
