-- udf_fill_buckets
CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets(
  input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
  buckets ARRAY<STRING>
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  (
    WITH total_counts AS (
      SELECT
        key,
        COALESCE(e.value, 0.0) AS value
      FROM
        UNNEST(buckets) AS key
      LEFT JOIN
        UNNEST(input_map) AS e
      ON
        SAFE_CAST(key AS STRING) = e.key
    )
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(SAFE_CAST(key AS STRING), value))
    FROM
      total_counts
  )
);
