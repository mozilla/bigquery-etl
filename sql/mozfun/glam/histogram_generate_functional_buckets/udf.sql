-- udf_functional_buckets
CREATE OR REPLACE FUNCTION glam.histogram_generate_functional_buckets(
  log_base INT64,
  buckets_per_magnitude INT64,
  range_max INT64
)
RETURNS ARRAY<FLOAT64> DETERMINISTIC
LANGUAGE js
AS
  '''
  function sample_to_bucket_index(sample) {
    // Get the index of the sample
    // https://github.com/mozilla/glean/blob/master/glean-core/src/histogram/functional.rs
    let exponent = Math.pow(log_base, 1.0/buckets_per_magnitude);
    return Math.ceil(Math.log(sample + 1) / Math.log(exponent));
  }

  let buckets = new Set([0]);
  for (let index = 0; index < sample_to_bucket_index(range_max); index++) {
    // Avoid re-using the exponent due to floating point issues when carrying
    // the `pow` operation e.g. `let exponent = ...; Math.pow(exponent, index)`.
    let bucket = Math.floor(Math.pow(log_base, index/buckets_per_magnitude));
    buckets.add(bucket);
  }

  return [...buckets]
''';
