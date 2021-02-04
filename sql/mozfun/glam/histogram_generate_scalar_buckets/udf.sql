-- udf_generate_buckets, except with FLOAT64 return type instead of STRING
CREATE OR REPLACE FUNCTION glam.histogram_generate_scalar_buckets(
  min_bucket FLOAT64,
  max_bucket FLOAT64,
  num_buckets INT64
)
RETURNS ARRAY<FLOAT64> DETERMINISTIC
LANGUAGE js
AS
  '''
  let bucket_size = (max_bucket - min_bucket) / num_buckets;
  let buckets = new Set();
  for (let bucket = min_bucket; bucket < max_bucket; bucket += bucket_size) {
    buckets.add(Math.pow(2, bucket).toFixed(2));
  }
  return Array.from(buckets);
''';

SELECT
  assert.array_equals([1, 2, 4, 8], glam.histogram_generate_scalar_buckets(0, log(16, 2), 4)),
  assert.array_equals(
    [1, 1.9, 3.62, 6.9, 13.13],
    glam.histogram_generate_scalar_buckets(0, log(25, 2), 5)
  )
