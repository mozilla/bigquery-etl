/*
Returns the most frequently occuring element in an array.
In the case of multiple values tied for the highest count, it returns the value
that appears latest in the array. Nulls are ignored.
See also: `stats.mode_last_retain_nulls`, which retains nulls.
*/
CREATE OR REPLACE FUNCTION stats.mode_last(list ANY TYPE) AS (
  (
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
      WITH OFFSET AS _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1
  )
);

-- Test
SELECT
  assert.equals('bar', stats.mode_last(['foo', 'bar', 'baz', 'bar', 'fred'])),
  assert.equals('baz', stats.mode_last(['foo', 'bar', 'baz', 'bar', 'baz', 'fred'])),
  assert.equals('foo', stats.mode_last([NULL, 'foo', NULL]));
