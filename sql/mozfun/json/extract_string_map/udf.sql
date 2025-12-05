CREATE OR REPLACE FUNCTION json.extract_string_map(input STRING)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  (
    SELECT
      IF(
        input_json IS NULL,
        NULL,
        (
          SELECT
            COALESCE(
              ARRAY_AGG(
                STRUCT(
                  key,
                  IF(
                    JSON_TYPE(input_json[key]) IN ('null', 'string'),
                    JSON_VALUE(input_json[key]),
                    TO_JSON_STRING(input_json[key])
                  ) AS value
                )
                ORDER BY
                  offset
              ),
              []
            )
          FROM
            UNNEST(JSON_KEYS(input_json, 1)) AS key
            WITH OFFSET
        )
      )
    FROM
      (SELECT SAFE.PARSE_JSON(input) AS input_json)
  )
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
    json.extract_string_map('{"a":"text","b":1,"c":null,"d":{},"e":[]}')
  ),
  assert.equals(0, ARRAY_LENGTH(json.extract_string_map('{}'))),
  assert.null(json.extract_string_map(NULL));
