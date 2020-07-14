-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf_js.glean_percentile(
  percentile FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>,
  type STRING
) AS (
  mozfun.glean.percentile(percentile, histogram, type)
);
