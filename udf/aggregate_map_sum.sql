/*

Returns the sum of non-null values by key in an array of maps.

Value type must be supported by SUM, which allows numeric data types INT64, NUMERIC, and FLOAT64.
In BigQuery the schema for a map is STRUCT<key_value ARRAY<STRUCT<key ANY TYPE, value ANY TYPE>>>.
In the case of repeated keys within a map, it returns the sum of all values for a key.

*/

CREATE TEMP FUNCTION
  udf_aggregate_map_sum(maps ANY TYPE) AS (STRUCT(ARRAY(
      SELECT
        AS STRUCT key,
        SUM(value) AS value
      FROM
        UNNEST(maps),
        UNNEST(key_value)
      GROUP BY
        key) AS key_value));

-- Tests

SELECT

  assert_array_equals([STRUCT('a' AS key, 3 AS value)],
    udf_aggregate_map_sum([
      STRUCT([STRUCT('a' AS key, 1 AS value)] AS key_value),
      STRUCT([STRUCT('a', 2)])
    ]).key_value),

  assert_array_equals([STRUCT(5 AS key, 20 AS value)],
    udf_aggregate_map_sum([
      STRUCT([STRUCT(5 AS key, 10 AS value), STRUCT(5, 10)] AS key_value)
    ]).key_value),

  assert_array_equals(
    udf_aggregate_map_sum([
        STRUCT([STRUCT(TRUE AS key, 1 AS value), STRUCT(TRUE, 1)] AS key_value),
        STRUCT([STRUCT(TRUE, 1), STRUCT(FALSE, NULL)]),
        STRUCT([STRUCT(CAST(NULL AS BOOL), 1)])
    ]).key_value,
    [STRUCT(TRUE AS key, 3 AS value),
     STRUCT(FALSE, null),
     STRUCT(null, 1)]);
