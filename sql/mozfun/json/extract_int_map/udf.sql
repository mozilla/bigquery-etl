/*

Returns an array of key/value structs from a string representing a JSON map.

Used by mozfun.hist.extract.

*/
CREATE OR REPLACE FUNCTION json.extract_int_map(input STRING) AS (
  ARRAY(
    SELECT
      STRUCT(
        SAFE_CAST(SPLIT(entry, ':')[OFFSET(0)] AS INT64) AS key,
        SAFE_CAST(SPLIT(entry, ':')[OFFSET(1)] AS INT64) AS value
      )
    FROM
      UNNEST(SPLIT(REPLACE(TRIM(input, '{}'), '"', ''), ',')) AS entry
    WHERE
      LENGTH(entry) > 0
  )
);

-- Tests
SELECT
  assert.array_equals(
    [
      STRUCT(0 AS key, 12434 AS value),
      STRUCT(1 AS key, 297 AS value),
      STRUCT(13 AS key, 8 AS value)
    ],
    json.extract_int_map('{"0":12434,"1":297,"13":8}')
  ),
  assert.equals(0, ARRAY_LENGTH(json.extract_int_map('{}'))),
  assert.array_equals(
    [STRUCT(1 AS key, NULL AS value)],
    json.extract_int_map('{"1":147573952589676410000}')
  );
