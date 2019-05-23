/*

Returns the most frequently occuring element in an array of json-compatible elements.

In the case of multiple values tied for the highest count, it returns the value
that appears latest in the array. Nulls are ignored.

*/

CREATE TEMP FUNCTION
  udf_json_mode_last(list ANY TYPE) AS ((
    SELECT
      ANY_VALUE(_value)
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS _offset
    GROUP BY
      TO_JSON_STRING(_value)
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1));

-- Tests

SELECT
  assert_equals(STRUCT('bar'), udf_json_mode_last([STRUCT('foo'), STRUCT('bar'), STRUCT('baz'), STRUCT('bar'), STRUCT('fred')])),
  assert_equals(STRUCT('baz'), udf_json_mode_last([STRUCT('foo'), STRUCT('bar'), STRUCT('baz'), STRUCT('bar'), STRUCT('baz'), STRUCT('fred')])),
  assert_equals(STRUCT('foo'), udf_json_mode_last([null, STRUCT('foo'), null]));
