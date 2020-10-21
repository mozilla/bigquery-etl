-- udf_buckets_to_map
CREATE OR REPLACE FUNCTION glam.histogram_from_buckets_empty(buckets ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given an array of values, transform them into a histogram MAP
  -- with the number of each key in the `buckets` array
  (SELECT ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, 1.0)) FROM UNNEST(buckets) AS bucket)
);
