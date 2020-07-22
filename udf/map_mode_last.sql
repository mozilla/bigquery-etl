-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.map_mode_last(entries ANY TYPE) AS (
  mozfun.map.mode_last(entries)
);

-- Tests

SELECT
  assert_array_equals([
      STRUCT('foo' AS key, 'bar' AS value),
      STRUCT('baz' AS key, 'fred' AS value)],
    udf.map_mode_last([
      STRUCT('foo' AS key, 'fred' AS value),
      STRUCT('foo' AS key, 'bar' AS value),
      STRUCT('baz' AS key, 'fred' AS value)
      ])),
  assert_array_equals([
      STRUCT('foo' AS key, 'bar' AS value),
      STRUCT('baz' AS key, 'fred' AS value)],
    udf.map_mode_last([
      STRUCT('foo' AS key, 'fred' AS value),
      STRUCT('foo' AS key, null AS value),
      STRUCT('foo' AS key, 'bar' AS value),
      STRUCT('foo' AS key, null AS value),
      STRUCT('baz' AS key, 'fred' AS value)
      ]))
