CREATE OR REPLACE FUNCTION udf_js.glean_percentile(
  percentile FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>,
  type STRING
)
RETURNS FLOAT64 DETERMINISTIC AS (
  glam.percentile(percentile, histogram, type)
);

SELECT
  assert.equals(
    2,
    udf_js.glean_percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
      "timing_distribution"
    )
  );

#xfail
SELECT
  udf_js.glean_percentile(
    101.0,
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
    "timing_distribution"
  );
