-- Definition for json.from_nested_map
CREATE OR REPLACE FUNCTION json.from_nested_map(input JSON)
RETURNS JSON AS (
  IF(
    ARRAY_LENGTH(JSON_KEYS(input, 1)) > 0,
    (
      SELECT
        JSON_OBJECT(
          ARRAY_AGG(key ORDER BY key_offset),
          ARRAY_AGG(
            IF(
              JSON_TYPE(input[key]) = 'array'
              AND ARRAY_LENGTH(JSON_QUERY_ARRAY(input[key])) > 0,
              (
                SELECT
                  JSON_OBJECT(
                    COALESCE(
                      ARRAY_AGG(JSON_VALUE(key_value_pair.key) ORDER BY key_value_pair_offset),
                      []
                    ),
                    COALESCE(ARRAY_AGG(key_value_pair.value ORDER BY key_value_pair_offset), [])
                  )
                FROM
                  UNNEST(JSON_QUERY_ARRAY(input[key])) AS key_value_pair
                  WITH OFFSET AS key_value_pair_offset
                WHERE
                  key_value_pair.key IS NOT NULL
                  AND key_value_pair.value IS NOT NULL
              ),
              input[key]
            )
            ORDER BY
              key_offset
          )
        )
      FROM
        UNNEST(JSON_KEYS(input, 1)) AS key
        WITH OFFSET AS key_offset
    ),
    input
  )
);

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
