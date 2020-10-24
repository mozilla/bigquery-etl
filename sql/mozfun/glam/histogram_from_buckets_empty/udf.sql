-- udf_buckets_to_map
CREATE OR REPLACE FUNCTION glam.histogram_from_buckets_empty(buckets ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given an array of values, transform them into a histogram MAP
  -- with the number of each key in the `buckets` array
  (
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, 1.0) ORDER BY CAST(bucket AS INT64))
    FROM
      UNNEST(buckets) AS bucket
  )
);

SELECT
  -- fill in 1 with a value of 0
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("2", 1.0), ("11", 1.0)],
    glam.histogram_from_buckets_empty(["11", "2"])
  )
