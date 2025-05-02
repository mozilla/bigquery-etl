CREATE OR REPLACE FUNCTION json.from_map_json(input JSON)
RETURNS JSON
LANGUAGE js
AS
  """
  if (input && input.length) {
    return input.reduce((acc, {key, value}) => {
      let parsed;
      try {
        parsed = JSON.parse(value);
      } catch (err) {
        parsed = value;
      }
      acc[key] = parsed;
      return acc;
    }, {});
  }
  return null;
""";

-- Tests
SELECT
  assert.null(json.from_map_json(NULL)),
  assert.null(json.from_map_json(JSON '[]')),
  assert.json_equals(
    JSON '{"foo": {"nested": 1}}',
    json.from_map_json(JSON '[{"key": "foo", "value": {"nested": 1}}]')
  ),
  assert.equals(TRUE, BOOL(json.from_map_json(JSON '[{"key": "foo", "value": "true"}]').foo)),
  assert.equals(123, INT64(json.from_map_json(JSON '[{"key": "foo", "value": "123"}]').foo))
