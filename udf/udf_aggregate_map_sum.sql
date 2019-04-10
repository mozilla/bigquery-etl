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

/*

Returns the sum of non-null values by key in an array of maps.

Value type must be supported by SUM, which allows numeric data types INT64, NUMERIC, and FLOAT64.

In BigQuery the schema for a map is STRUCT<key_value ARRAY<STRUCT<key ANY TYPE, value ANY TYPE>>>.

In the case of repeated keys within a map, it returns the sum of all values for a key.

Examples:

-- key a appears in both maps
SELECT udf_map_sum([
  STRUCT([
    STRUCT('a' AS key, 1 AS value)
  ] AS key_value),
  STRUCT([
    STRUCT('a', 2)
  ])
]);
-- {"a":3}

SELECT udf_map_sum([
  STRUCT([
    STRUCT(5 AS key, 10 AS value),
    STRUCT(5, 10)
  ] AS key_value)
]);
-- {5: 20}

SELECT udf_map_sum([
  STRUCT([
    STRUCT(true AS key, 1 AS value),
    STRUCT(true, 1)
  ] AS key_value),
  STRUCT([
    STRUCT(true, 1),
    STRUCT(false, NULL)
  ]),
  STRUCT([
    STRUCT(CAST(NULL AS BOOL), 1)
  ])
]);
-- {true: 3: false: null, null: 1}

*/
