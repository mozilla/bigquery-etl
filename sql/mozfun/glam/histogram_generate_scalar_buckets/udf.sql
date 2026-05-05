-- udf_generate_buckets, except with FLOAT64 return type instead of STRING
CREATE OR REPLACE FUNCTION glam.histogram_generate_scalar_buckets(
  min_bucket FLOAT64,
  max_bucket FLOAT64,
  num_buckets INT64
)
RETURNS ARRAY<FLOAT64> AS (
  IF(
    min_bucket >= max_bucket,
    [],
    ARRAY(
      SELECT
        ROUND(POW(2, (max_bucket - min_bucket) / num_buckets * val), 2)
      FROM
        UNNEST(GENERATE_ARRAY(0, num_buckets - 1)) AS val
    )
  )
);

SELECT
  assert.array_equals([1, 2, 4, 8], glam.histogram_generate_scalar_buckets(0, LOG(16, 2), 4)),
  assert.array_equals(
    [1, 1.9, 3.62, 6.9, 13.13],
    glam.histogram_generate_scalar_buckets(0, LOG(25, 2), 5)
  ),
  assert.array_equals([], glam.histogram_generate_scalar_buckets(10, 10, 100))
