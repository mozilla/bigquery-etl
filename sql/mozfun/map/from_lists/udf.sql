CREATE OR REPLACE FUNCTION map.from_lists(keys ANY TYPE, `values` ANY TYPE)
RETURNS ARRAY<STRUCT<key STRING, value STRING>> AS (
  ARRAY(
    SELECT AS STRUCT
      CAST(key AS STRING) AS key,
      CAST(`values`[off] AS STRING) AS value,
    FROM
      UNNEST(keys) AS key
      WITH OFFSET off
    WHERE
      off < ARRAY_LENGTH(`values`)
  )
);

-- Test
SELECT
  mozfun.assert.map_equals([STRUCT("a" AS key, "1" AS value)], map.from_lists(["a"], ["1"])),
  mozfun.assert.map_equals([STRUCT("a" AS key, "1" AS value)], map.from_lists(["a"], [1])),
  mozfun.assert.map_equals(
    [STRUCT("a" AS key, "1" AS value), STRUCT("b" AS key, "2" AS value)],
    map.from_lists(["a", "b"], [1, 2])
  ),
  mozfun.assert.map_equals([STRUCT("a" AS key, "1" AS value)], map.from_lists(["a", "b"], [1])),
  mozfun.assert.equals(0, ARRAY_LENGTH(map.from_lists(CAST(NULL AS ARRAY<STRING>), [1]))),
  mozfun.assert.equals(0, ARRAY_LENGTH(map.from_lists([], [1]))),
  mozfun.assert.equals(0, ARRAY_LENGTH(map.from_lists([1], CAST(NULL AS ARRAY<STRING>)))),
  mozfun.assert.equals(0, ARRAY_LENGTH(map.from_lists([1], []))),
