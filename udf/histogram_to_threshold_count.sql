-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.histogram_to_threshold_count(histogram STRING, threshold INT64) AS (
  mozfun.hist.threshold_count(histogram STRING, threshold INT64)
);
