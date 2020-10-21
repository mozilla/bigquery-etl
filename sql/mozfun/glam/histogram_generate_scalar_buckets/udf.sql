-- udf_generate_buckets
CREATE OR REPLACE FUNCTION glam.histogram_generate_scalar_buckets(
  min_bucket FLOAT64,
  max_bucket FLOAT64,
  num_buckets INT64
)
RETURNS ARRAY<STRING> DETERMINISTIC
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
