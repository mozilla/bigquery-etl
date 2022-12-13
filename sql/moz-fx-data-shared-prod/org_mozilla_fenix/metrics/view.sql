-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_fenix.metrics`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        mozdata.udf.normalize_fenix_metrics(client_info.telemetry_sdk_build, metrics).* REPLACE (
          STRUCT(
            mozfun.glean.parse_datetime(
              metrics.datetime.glean_validation_first_run_hour
            ) AS glean_validation_first_run_hour,
            metrics.datetime.glean_validation_first_run_hour AS raw_glean_validation_first_run_hour
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
