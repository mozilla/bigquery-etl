-- Definition for json.from_nested_map
CREATE OR REPLACE FUNCTION json.from_nested_map(input JSON)
RETURNS json
LANGUAGE js
AS
  """
  if (input && Object.keys(input).length) {
    for (const k in input) {
      if (input[k] && Array.isArray(input[k]) && input[k].length) {
        input[k] = input[k].reduce((acc, {key, value}) => {
          acc[key] = value;
          return acc;
        }, {});
      }
    }
  }
  return input;
""";

-- Tests
SELECT
  assert.null(json.from_nested_map(NULL)),
  assert.json_equals(JSON '{ "metric": [] }', json.from_nested_map(JSON '{ "metric": [] }')),
  assert.json_equals(JSON '{ "metric": {} }', json.from_nested_map(JSON '{ "metric": {} }')),
  assert.json_equals(
    JSON '{ "metric": { "extra": 2 } }',
    json.from_nested_map(JSON '{ "metric": [ {"key": "extra", "value": 2 } ] }')
  ),
  assert.json_equals(JSON '{ "metric": {}}', json.from_nested_map(JSON '{ "metric": [1, 2, 3]}')),
  assert.json_equals(
    JSON '{ "metric": "value"}',
    json.from_nested_map(JSON '{ "metric": "value"}')
  ),
  assert.json_equals(
    JSON '{ "metric": {}}',
    json.from_nested_map(JSON '{ "metric": [{"key": "value"}]}')
  )
