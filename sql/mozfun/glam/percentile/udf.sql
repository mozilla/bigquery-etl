-- udf_js.glean_percentile
CREATE OR REPLACE FUNCTION glam.percentile(
  percentile FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>,
  type STRING
)
RETURNS FLOAT64 DETERMINISTIC
LANGUAGE js
AS
  '''
  if (percentile < 0 || percentile > 100) {
      throw "percentile must be a value between 0 and 100";
  }

  let values = histogram.map(bucket => bucket.value);
  let total = values.reduce((a, b) => a + b);
  let normalized = values.map(value => value / total);

  // Find the index into the cumulative distribution function that corresponds
  // to the percentile. This undershoots the true value of the percentile.
  let acc = 0;
  let index = null;
  for (let i = 0; i < normalized.length; i++) {
      acc += normalized[i];
      index = i;
      if (acc >= percentile / 100) {
          break;
      }
  }

  // NOTE: we do not perform geometric or linear interpolation, but this would
  // be the place to implement it.
  return histogram[index].key;
''';

SELECT
  assert.equals(
    2,
    glam.percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
      "timing_distribution"
    )
  );

#xfail
SELECT
  glam.percentile(
    101.0,
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
    "timing_distribution"
  );
