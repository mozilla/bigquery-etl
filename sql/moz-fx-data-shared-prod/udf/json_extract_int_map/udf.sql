-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.json_extract_int_map(input STRING) AS (
  mozfun.json.extract_int_map(input)
);
