-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.gzip_length_footer(compressed BYTES) AS (
  mozfun.gzip.footer_length(compressed )
);
