-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.normalize_os(os STRING) AS (
  mozfun.norm.os(os)
);
