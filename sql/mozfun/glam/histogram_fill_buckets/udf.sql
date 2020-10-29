-- udf_fill_buckets
CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets(
  input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
  buckets ARRAY<STRING>
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  ARRAY(
    SELECT AS STRUCT
      key,
      COALESCE(e.value, 0.0) AS value
    FROM
      UNNEST(buckets) AS key
    LEFT JOIN
      UNNEST(input_map) AS e
    ON
      key = e.key
    ORDER BY
      key
  )
);

SELECT
  -- fill in 1 with a value of 0
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("1", 0.0), ("2", 1.0)],
    glam.histogram_fill_buckets(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("2", 1.0)],
      ["0", "1", "2"]
    )
  ),
  -- only keep values in specified in buckets
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0)],
    glam.histogram_fill_buckets(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("2", 1.0)],
      ["0"]
    )
  ),
  -- return ordered keys by string value
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("11", 0.0), ("2", 1.0)],
    glam.histogram_fill_buckets(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("2", 1.0)],
      ["11", "2"]
    )
  )
