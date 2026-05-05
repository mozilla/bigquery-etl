/*

Returns a JSON string which has the `pair` appended to the provided `input` JSON string.
NULL is also valid for `input`.

Examples:

  udf.kv_array_append_to_json_string('{"foo":"bar"}', [STRUCT("baz" AS key, "boo" AS value)])

  '{"foo":"bar","baz":"boo"}'

  udf.kv_array_append_to_json_string('{}', [STRUCT("baz" AS key, "boo" AS value)])

  '{"baz": "boo"}'

*/
CREATE OR REPLACE FUNCTION udf.kv_array_append_to_json_string(input STRING, arr ANY TYPE) AS (
  CONCAT(
    IF(input IS NULL OR input = "{}", "{", CONCAT(RTRIM(input, "}"), ",")),
    LTRIM(udf.kv_array_to_json_string(arr), "{")
  )
);

-- Test
SELECT
  mozfun.assert.equals(
    '{"hello":"world","foo":"bar","baz":"boo"}',
    udf.kv_array_append_to_json_string(
      '{"hello":"world"}',
      [STRUCT("foo" AS key, "bar" AS value), STRUCT("baz" AS key, "boo" AS value)]
    )
  ),
  mozfun.assert.equals(
    '{"foo":"bar"}',
    udf.kv_array_append_to_json_string(CAST(NULL AS STRING), [STRUCT("foo" AS key, "bar" AS value)])
  ),
  mozfun.assert.equals(
    '{"foo":"bar"}',
    udf.kv_array_append_to_json_string('{}', [STRUCT("foo" AS key, "bar" AS value)])
  );
