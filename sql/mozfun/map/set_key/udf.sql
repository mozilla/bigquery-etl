CREATE OR REPLACE FUNCTION map.set_key(map ANY TYPE, new_key ANY TYPE, new_value ANY TYPE) AS (
  IF(
    new_key IS NULL,
    ERROR(
      "mozfun.map.set_key: Cannot insert NULL keys into map, tried to insert: (key=" || CAST(
        new_key AS STRING
      ) || ", value=" || CAST(new_value AS STRING) || ")"
    ),
    ARRAY(
      SELECT
        STRUCT(key, COALESCE(new_data.value, existing_data.value) AS value)
      FROM
        (SELECT new_key AS key, new_value AS value) AS new_data
      FULL OUTER JOIN
        -- BQ doesn't allow you to FULL OUTER JOIN an
        -- unnested array directly, so we use a subquery
        (SELECT * FROM UNNEST(map)) AS existing_data
        USING (key)
    )
  )
);

-- Tests
SELECT
  -- No current map
  mozfun.assert.map_entries_equals(
    map.set_key(CAST([] AS ARRAY<STRUCT<key STRING, value INT64>>), "a", 1),
    [STRUCT("a" AS key, 1 AS value)]
  ),
  -- Null current map
  mozfun.assert.map_entries_equals(
    map.set_key(CAST(NULL AS ARRAY<STRUCT<key STRING, value INT64>>), "a", 1),
    [STRUCT("a" AS key, 1 AS value)]
  ),
  -- Have current map, key is not present
  mozfun.assert.map_entries_equals(
    map.set_key([STRUCT("b" AS key, 2 AS value)], "a", 1),
    [STRUCT("a" AS key, 1 AS value), STRUCT("b", 2)]
  ),
  -- Have current map, key is present
  mozfun.assert.map_entries_equals(
    map.set_key([STRUCT("a" AS key, 2 AS value)], "a", 1),
    [STRUCT("a" AS key, 1 AS value)]
  ),
