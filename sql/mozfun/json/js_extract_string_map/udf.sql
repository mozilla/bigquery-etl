CREATE OR REPLACE FUNCTION json.js_extract_string_map(input STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>>
LANGUAGE js
AS
  """
const obj = JSON.parse(input)
if (obj === null) {
  return null;
}
return Object.entries(obj).map(([key, value]) => {
  if (value === null || typeof value === 'string') {
    return {key, value};
  } else {
    return {key, value: JSON.stringify(value)};
  }
});
""";

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
