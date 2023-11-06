CREATE OR REPLACE FUNCTION map.set_key(map ANY TYPE, new_key ANY TYPE, new_value ANY TYPE)
AS (
  ARRAY(
    SELECT
      STRUCT(
        key,
        COALESCE(new_value, value)
      )
    FROM (
      SELECT new_key AS key, new_value
    )
    FULL OUTER JOIN
      -- BQ doesn't allow you to FULL OUTER JOIN an
      -- unnested array directly, so we use a subquery
      (SELECT * FROM UNNEST(map)) AS existing_data
      USING (key)
  )
);


-- Tests
SELECT
  -- No current map
  assert.map_entries_equals(map.set_key(CAST(NULL AS ARRAY<STRUCT<key STRING, value INT64>>), "a", 1), [STRUCT("a" AS key, 1 AS value)]),

  -- Have current map, key is not present
  assert.map_entries_equals(map.set_key([STRUCT("b" AS key, 2 AS value)], "a", 1), [STRUCT("a" AS key, 1 AS value), STRUCT("b", 2)]),

  -- Have current map, key is present
  assert.map_entries_equals(map.set_key([STRUCT("a" AS key, 2 AS value)], "a", 1), [STRUCT("a" AS key, 1 AS value)]),

  -- Null values for keys in map
  assert.map_entries_equals(map.set_key([STRUCT(CAST(NULL AS STRING) AS key, 1 AS value)], "a", 1), [STRUCT("a" AS key, 1 AS value), STRUCT(CAST(NULL AS STRING), 1)]),

  -- Null values for new key
  assert.map_entries_equals(map.set_key([STRUCT("a" AS key, 1 AS value)], CAST(NULL AS STRING), 1), [STRUCT("a" AS key, 1 AS value), STRUCT(CAST(NULL AS STRING), 1)])
