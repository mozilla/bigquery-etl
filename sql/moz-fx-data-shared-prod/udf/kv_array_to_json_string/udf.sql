/*

Returns a JSON string representing the input key-value array.

Value type must be able to be represented as a string - this function will cast to a string.
At Mozilla, the schema for a map is STRUCT<key_value ARRAY<STRUCT<key ANY TYPE, value ANY TYPE>>>.
To use this with that representation, it should be as `udf.kv_array_to_json_string(struct.key_value)`.

*/
CREATE OR REPLACE FUNCTION udf.kv_array_to_json_string(kv_arr ANY TYPE) AS (
  (
    SELECT
      CONCAT(
        '{',
        ARRAY_TO_STRING(
          ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')),
          ","
        ),
        '}'
      )
    FROM
      UNNEST(kv_arr)
  )
);

-- Test
SELECT
  mozfun.assert.equals(
    '{"hello":"1","world":"2"}',
    udf.kv_array_to_json_string(
      [STRUCT('hello' AS key, 1 AS value), STRUCT('world' AS key, 2 AS value)]
    )
  ),
  mozfun.assert.equals(
    '{"one-entry":"1"}',
    udf.kv_array_to_json_string([STRUCT('one-entry' AS key, 1 AS value)])
  );
