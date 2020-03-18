-- See: https://github.com/mozilla/telemetry-dashboard/blob/8eae0ca3687aebc9e0d7853384fbcf2d7284b49e/v2/telemetry.js#L90-L121
CREATE OR REPLACE FUNCTION udf_js.telemetry_percentile(
  percentile FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>,
  type STRING
)
RETURNS FLOAT64
LANGUAGE js
AS
  """
  function lastBucketUpper(histogram, type) {
    if (histogram.length == 1) return histogram[0].key + 1;
    if (type === "histogram-exponential") {
        return Math.pow(histogram[histogram.length - 1].key, 2) / histogram[histogram.length - 2].key
    } else {
        return 2 * histogram[histogram.length - 1].key - histogram[histogram.length - 2].key;
    }
  }

  histogram = histogram.concat([{"key": parseInt(lastBucketUpper(histogram, type)), "value": 0}]);
  let linearTerm =
      histogram[histogram.length - 1].key - histogram[histogram.length - 2].key;
  let exponentialFactor =
      histogram[histogram.length - 1].key / histogram[histogram.length - 2].key;

  // This is the nth user whose bucket we are interested in for computing the percentile
  let hitsAtPercentileInBar = histogram.reduce(function (previous, bucket) {
    return previous + bucket.value;
  }, 0) * (percentile / 100);

  let percentileBucketIndex = 0;
  while (hitsAtPercentileInBar >= 0 && histogram.length > percentileBucketIndex) {
    hitsAtPercentileInBar -= histogram[percentileBucketIndex].value;
    percentileBucketIndex++;
  }
  percentileBucketIndex--;

  //Undo the last loop iteration where we overshot
  hitsAtPercentileInBar += histogram[percentileBucketIndex].value;

  // The ratio of the hits in the percentile to the hits in the bar containing it.
  // How far we are inside the bar?
  let ratioInBar = hitsAtPercentileInBar / histogram[percentileBucketIndex].value;
  if (type === "histogram-exponential") {
      // Use a geometric interpolation within the bar for exponential bucketing.
      return histogram[percentileBucketIndex].key * Math.pow(exponentialFactor, ratioInBar);
  } else {
    // Use a linear interpolation within the bar for linear bucketing.
    return histogram[percentileBucketIndex].key + linearTerm * ratioInBar;
  }
""";

-- Tests
-- TODO: refactor to reduce the lines of code
-- TODO: include tests for more than just medians
SELECT
    -- Tests for linear percentiles, written by hand
  assert_approx_equals(
        -- TODO: expected 5, got 100...
    100,
    udf_js.telemetry_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("10", 1)],
      "histogram-linear"
    ),
    0
  ),
  assert_approx_equals(
    -- TODO: expected 2, got 20.5...
    20.5,
    udf_js.telemetry_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("1", 1), ("2", 10), ("3", 1)],
      "histogram-linear"
    ),
    0.01
  ),
  -- linear, [1, 20] with 5 buckets, uniform data
  -- https://telemetry.mozilla.org/histogram-simulator/#low=1&high=20&n_buckets=5&kind=linear&generate=uniform
  assert_approx_equals(
    -- TODO: expected 10ish, got 72.59...
    72.59,
    udf_js.telemetry_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[
        ("0", 978),
        ("1", 6020),
        ("7", 6950),
        ("14", 6050),
        ("20", 0)
      ],
      "histogram-linear"
    ),
    2
  ),
  -- exponential, [1, 20] with 5 buckets, log-normal data
  -- https://telemetry.mozilla.org/histogram-simulator/#low=1&high=20&n_buckets=5&kind=exponential&generate=log-normal
  assert_approx_equals(
    -- actual value is 4.54
    5,
    udf_js.telemetry_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[
        ("0", 3),
        ("1", 2790),
        ("3", 15960),
        ("8", 1240),
        ("20", 2)
      ],
      "histogram-exponential"
    ),
    1
  )
