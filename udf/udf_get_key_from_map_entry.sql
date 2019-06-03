/*

Return the value for map entry if entry key matches the given key, otherwise NULL.

In BigQuery the schema for a map entry is STRUCT<key ANY TYPE, value ANY TYPE>.
This is useful for aggregating a key from an unnested map column, as seen in the test.

*/

CREATE TEMP FUNCTION
  udf_get_key_from_map_entry(entry ANY TYPE,
    key ANY TYPE) AS (
  IF
    (entry.key = key,
      entry.value,
      NULL));

-- Tests

SELECT
  assert_equals(SUM(udf_get_key_from_map_entry(entry, 'key1')), 10),
  assert_equals(SUM(udf_get_key_from_map_entry(entry, 'key2')), 20),
  assert_equals(SUM(udf_get_key_from_map_entry(entry, 'key3')), 30)
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
    STRUCT([STRUCT('key1', 1), STRUCT('key2', 2), STRUCT('key3', 3)])]) AS map,
  UNNEST(map.key_value) AS entry
