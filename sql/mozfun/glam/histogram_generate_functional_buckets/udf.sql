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

    // NOTE: the sample_to_bucket_index implementation overshoots the true index, 
    // so we break out early if we hit the max bucket range.
    if (bucket > range_max) {
      break;
    }
    buckets.add(bucket);
  }

  return [...buckets]
''';

SELECT
  -- First 50 keys of a timing distribution
  -- https://mozilla.github.io/glean/book/user/metrics/timing_distribution.html
  -- https://sql.telemetry.mozilla.org/queries/75805/source
  assert.array_equals(
    [
      --format:off
      0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,19,20,22,24,26,29,32,34,38,41,
      45,49,53,58,64,69,76,82,90,98,107,117,128,139,152,165,181,197,215,234,
      256,279,304
      --format:on
    ],
    glam.histogram_generate_functional_buckets(2, 8, 305)
  ),
  -- Check for existance of first 10 keys of a memory distribution
  -- https://mozilla.github.io/glean/book/user/metrics/memory_distribution.html
  -- https://sql.telemetry.mozilla.org/queries/75805/source
  assert.equals(
    10,
    (
      SELECT
        count(*)
      FROM
        UNNEST(
          [440871, 460390, 571740, 597053, 623487, 651091, 679917, 710019, 741455, 774282]
        ) expect
      JOIN
        UNNEST(glam.histogram_generate_functional_buckets(2, 16, 774282 + 1)) actual
      WHERE
        expect = actual
    )
  )
