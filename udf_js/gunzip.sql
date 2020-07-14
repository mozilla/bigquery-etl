-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf_js.gunzip(input BYTES) AS (
  mozfun.gzip.decompress(input)
);
