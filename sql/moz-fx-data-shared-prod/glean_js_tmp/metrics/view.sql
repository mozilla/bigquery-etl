CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_js_tmp.metrics`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info)
FROM
  `moz-fx-data-shared-prod.glean_js_tmp_stable.metrics_v1`
