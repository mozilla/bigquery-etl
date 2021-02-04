-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.normalize_glean_ping_info(ping_info ANY TYPE) AS (
  mozfun.norm.glean_ping_info(ping_info)
);
