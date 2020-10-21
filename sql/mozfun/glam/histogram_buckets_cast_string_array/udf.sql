-- udf_to_string_arr
CREATE OR REPLACE FUNCTION glam.histogram_buckets_cast_string_array(buckets ARRAY<INT64>)
RETURNS ARRAY<STRING> AS (
  (SELECT ARRAY_AGG(CAST(bucket AS STRING)) FROM UNNEST(buckets) AS bucket)
);
