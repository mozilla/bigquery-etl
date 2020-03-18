-- See: https://github.com/mozilla/telemetry-dashboard/blob/8eae0ca3687aebc9e0d7853384fbcf2d7284b49e/v2/telemetry.js#L90-L121
CREATE OR REPLACE FUNCTION udf_js.telemetry_percentile(
  percentile FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>,
  type STRING
)
RETURNS FLOAT64
LANGUAGE js
AS
  '''
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

  // The ratio of the hits in the percentile to the hits in the bar containing it - how far we are inside the bar
  let ratioInBar = hitsAtPercentileInBar / histogram[percentileBucketIndex].value;
  if (type = 'histogram-exponential') { // exponential buckets
      return histogram[percentileBucketIndex].key * Math.pow(
      exponentialFactor, ratioInBar); // geometric interpolation within bar
  } else { // linear buckets
    return histogram[percentileBucketIndex].key + linearTerm * ratioInBar; // linear interpolation within bar
  }
''';

-- Tests
-- TODO: refactor to reduce the lines of code
SELECT
  -- Determine the halfway point between two buckets
  assert_equals(
    5,
    udf_js.telemetry_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("10", 1)],
      "histogram-linear"
    )
  ),
  assert_approx_equals(
    1.5,
    udf_js.telemetry_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("1", 1), ("2", 1)],
      "histogram-linear"
    ),
    0.01
  ),
  assert_approx_equals(
    50,
    udf_js.telemetry_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("1", 1), ("100", 1)],
      "histogram-linear"
    ),
    0.01
  )
