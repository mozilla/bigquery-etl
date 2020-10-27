-- udf_to_string_arr
CREATE OR REPLACE FUNCTION glam.histogram_buckets_cast_string_array(buckets ARRAY<INT64>)
RETURNS ARRAY<STRING> AS (
  (SELECT ARRAY_AGG(CAST(bucket AS STRING) ORDER BY bucket) FROM UNNEST(buckets) AS bucket)
);

SELECT
  assert.array_equals(["0", "1", "2"], glam.histogram_buckets_cast_string_array([0, 1, 2])),
  assert.array_equals(["0", "1", "2"], glam.histogram_buckets_cast_string_array([0, 2, 1]))
