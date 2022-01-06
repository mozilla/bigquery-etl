-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.metrics`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    mozdata.udf.normalize_fenix_metrics(client_info.telemetry_sdk_build, metrics) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
