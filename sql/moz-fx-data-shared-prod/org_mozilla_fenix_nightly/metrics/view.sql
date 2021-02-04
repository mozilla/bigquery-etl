CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.metrics`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    `moz-fx-data-shared-prod.udf.normalize_fenix_metrics`(client_info.telemetry_sdk_build, metrics) AS metrics)
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.metrics_v1`
