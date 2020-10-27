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

--Tests
SELECT
  assert.equals(2.0, glam.histogram_bucket_from_value(["1", "2", "3"], 2.333)),
  assert.equals(NULL, glam.histogram_bucket_from_value(["1"], 0.99)),
  assert.equals(0.0, glam.histogram_bucket_from_value(["0", "1"], 0.99)),
  assert.equals(0.0, glam.histogram_bucket_from_value(["1", "0"], 0.99)),
