/*

Return the first value for a given key in a map.

The BigQuery type that results from loading a parquet map is STRUCT<key_value ARRAY<STRUCT<key ANY TYPE, value ANY TYPE>>>.

*/

CREATE TEMP FUNCTION
  udf_get_key(map ANY TYPE,
    k ANY TYPE) AS ((
    SELECT
      key_value.value
    FROM
      UNNEST(map.key_value) AS key_value
    WHERE
      key_value.key = k
    LIMIT
      1));

-- Tests

SELECT
  assert_equals(SUM(udf_get_key(map, 'key1')), 10),
  assert_equals(SUM(udf_get_key(map, 'key2')), 20),
  assert_equals(SUM(udf_get_key(map, 'key3')), 30)
FROM
  UNNEST([ --
    STRUCT([STRUCT('key1' AS key, 1 AS value), STRUCT('key2', 2), STRUCT('key3', 3)] AS key_value),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)]),
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)])]) AS map
