-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.histogram_to_mean(histogram ANY TYPE) AS (
  mozfun.hist.mean(histogram)
);
