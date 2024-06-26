-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.ads_backend.events`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* REPLACE (
          STRUCT(
            mozfun.glean.parse_datetime(
              metrics.datetime.technical_operations_fetched_timestamp
            ) AS technical_operations_fetched_timestamp,
            metrics.datetime.technical_operations_fetched_timestamp AS raw_technical_operations_fetched_timestamp,
            mozfun.glean.parse_datetime(
              metrics.datetime.technical_operations_served_timestamp
            ) AS technical_operations_served_timestamp,
            metrics.datetime.technical_operations_served_timestamp AS raw_technical_operations_served_timestamp
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.ads_backend_stable.events_v1`
