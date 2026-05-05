CREATE OR REPLACE FUNCTION json.from_map_json(input JSON)
RETURNS JSON AS (
  IF(
    ARRAY_LENGTH(SAFE.JSON_QUERY_ARRAY(input)) > 0,
    (
      SELECT
        JSON_OBJECT(
          COALESCE(ARRAY_AGG(JSON_VALUE(key_value_pair.key) ORDER BY offset), []),
          COALESCE(
            ARRAY_AGG(
              COALESCE(SAFE.PARSE_JSON(SAFE.STRING(key_value_pair.value)), key_value_pair.value)
              ORDER BY
                offset
            ),
            []
          )
        )
      FROM
        UNNEST(JSON_QUERY_ARRAY(input)) AS key_value_pair
        WITH OFFSET
      WHERE
        key_value_pair.key IS NOT NULL
        AND key_value_pair.value IS NOT NULL
    ),
    NULL
  )
);

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
