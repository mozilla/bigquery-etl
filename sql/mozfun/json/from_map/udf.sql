-- Definition for json.from_map
CREATE OR REPLACE FUNCTION json.from_map(input ANY type)
RETURNS json AS (
  json.from_map_json(TO_JSON(input))
);

-- Tests
SELECT
  assert.null(json.from_map(NULL)),
  assert.null(json.from_map([])),
  assert.json_equals(
    JSON '{"foo": {"nested": 1}}',
    json.from_map([STRUCT("foo" AS key, STRUCT(1 AS nested) AS value)])
  ),
  assert.equals(LAX_BOOL(json.from_map([STRUCT("foo" AS key, "True" AS value)]).foo), TRUE),
  assert.equals(BOOL(json.from_map([STRUCT("foo" AS key, "true" AS value)]).foo), TRUE),
  assert.equals(INT64(json.from_map([STRUCT("foo" AS key, "123" AS value)]).foo), 123)
