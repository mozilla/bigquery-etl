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

/*

Returns the most frequently occuring element in an array.

In the case of multiple values tied for the highest count, it returns the value
that appears latest in the array. Nulls are ignored.

Examples:

SELECT udf_mode_last(['foo', 'bar', 'baz', 'bar', 'fred']);
-- bar

SELECT udf_mode_last(['foo', 'bar', 'baz', 'bar', 'baz', 'fred']);
-- baz

SELECT udf_mode_last([null, 'foo', null]);
-- foo

*/
