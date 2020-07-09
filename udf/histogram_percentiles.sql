-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.histogram_percentiles(
  histogram ANY TYPE,
  percentiles ARRAY<FLOAT64>
) AS (
  mozfun.hist.percentiles(histogram ANY TYPE, percentiles ARRAY<FLOAT64>)
);
