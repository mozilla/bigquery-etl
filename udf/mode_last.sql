/*

Returns the most frequently occuring element in an array.

In the case of multiple values tied for the highest count, it returns the value
that appears latest in the array. Nulls are ignored.

*/

CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));

-- Test

SELECT
  assert_equals('bar', udf_mode_last(['foo', 'bar', 'baz', 'bar', 'fred'])),
  assert_equals('baz', udf_mode_last(['foo', 'bar', 'baz', 'bar', 'baz', 'fred'])),
  assert_equals('foo', udf_mode_last([null, 'foo', null]));
