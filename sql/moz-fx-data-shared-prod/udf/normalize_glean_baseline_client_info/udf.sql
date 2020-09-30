-- Legacy wrapper around a function moved to mozfun.
CREATE OR REPLACE FUNCTION udf.normalize_glean_baseline_client_info(
  client_info ANY TYPE,
  metrics ANY TYPE
) AS (
  mozfun.norm.glean_baseline_client_info(client_info, metrics)
);
