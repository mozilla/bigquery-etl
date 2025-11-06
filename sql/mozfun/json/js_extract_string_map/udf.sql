CREATE OR REPLACE FUNCTION json.js_extract_string_map(input STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  json.extract_string_map(input)
);

-- Tests
SELECT
  assert.array_equals(
    [
      STRUCT("a" AS key, "text" AS value),
      STRUCT("b" AS key, "1" AS value),
      STRUCT("c" AS key, NULL AS value),
      STRUCT("d" AS key, "{}" AS value),
      STRUCT("e" AS key, "[]" AS value)
    ],
    json.js_extract_string_map('{"a":"text","b":1,"c":null,"d":{},"e":[]}')
  ),
  assert.equals(0, ARRAY_LENGTH(json.js_extract_string_map('{}'))),
  assert.null(json.js_extract_string_map(NULL));
