-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.json_extract_histogram(input STRING) AS (
  mozfun.hist.extract(input)
);
