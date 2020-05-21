/*

Returns a JSON string which has the `pair` appended to the provided `input` JSON string.

Example:
  udf_kv_array_append_to_json_string('{"foo":"bar"}', [STRUCT("baz" AS key, "boo" AS value)])

  '{"foo":"bar","baz":"boo"}'

  udf_kv_array_append_to_json_string('{}', [STRUCT("baz" AS key, "boo" AS value)])

  '{"baz": "boo"}'

*/
CREATE OR REPLACE FUNCTION udf.kv_array_append_to_json_string(input STRING, arr ANY TYPE) AS (
  CONCAT(
    RTRIM(input, "}"),
    IF(input = "{}", "", ","),
    TRIM(`moz-fx-data-shared-prod.udf.kv_array_to_json_string`(arr), "{")
  )
);

-- Test
SELECT
  assert_equals(
    '{"hello":"1","world":"2","foo":"bar"}',
    udf.kv_array_append_to_json_string(
      '{"hello":"1"}',
      [STRUCT('world' AS key, 2 AS value), STRUCT('foo' AS key, "bar" AS value)]
    )
  ),
  assert_equals(
    '{"foo":"bar"}',
    udf.kv_array_append_to_json_string('{}', [STRUCT('foo' AS key, 'bar' AS value)])
  );
