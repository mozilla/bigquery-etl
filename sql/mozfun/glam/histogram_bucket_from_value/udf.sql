-- udf_bucket
CREATE OR REPLACE FUNCTION glam.histogram_bucket_from_value(buckets ARRAY<STRING>, val FLOAT64)
RETURNS FLOAT64 AS (
  -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
  (
    SELECT
      MAX(CAST(bucket AS FLOAT64))
    FROM
      UNNEST(buckets) AS bucket
    WHERE
      val >= CAST(bucket AS FLOAT64)
  )
);
