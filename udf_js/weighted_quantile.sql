CREATE OR REPLACE FUNCTION udf_js.weighted_quantile(
  percentile FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>
)
RETURNS FLOAT64
LANGUAGE js
AS
  '''
  if (percentile < 0 || percentile > 100) {
      throw "percentile must be a value between 0 and 100";
  }

  let keys = histogram.map(bucket => parseInt(bucket.key));
  let values = histogram.map(bucket => bucket.value);

  return weightedQuantile([percentile/100], keys, values)[0];
'''
OPTIONS
  (library = "gs://moz-fx-data-circleci-tests-bigquery-etl/wtdstats.js");

SELECT
  -- centered
  assert_equals(
    1,
    udf_js.weighted_quantile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 10), ("1", 10), ("2", 10)],
      "timing_distribution"
    )
  ),
  -- skew right
  assert_equals(
    2,
    udf_js.weighted_quantile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 10), ("1", 10), ("2", 100)],
      "timing_distribution"
    )
  ),
  -- skew left
  assert_equals(
    0,
    udf_js.weighted_quantile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 100), ("1", 10), ("2", 10)],
      "timing_distribution"
    )
  ),
    -- Bug when all weights are less than 2. This test fails with a `400
    -- Expected "NaN" but got "NaN"`.
  -- assert_equals(
  --   CAST("NaN" as FLOAT64),
  --   udf_js.weighted_quantile(
  --     50.0,
  --     ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("1", 1), ("2", 1)],
  --     "timing_distribution"
  --   )
  -- ),
  -- expected behavior of the above by reweighting
  assert_equals(
    1,
    udf_js.weighted_quantile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 3), ("1", 3), ("2", 3)],
      "timing_distribution"
    )
  ),;

#xfail
SELECT
  udf_js.glean_percentile(
    101.0,
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("1", 2), ("2", 1)],
    "timing_distribution"
  );
