-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_lockbox.addresses_sync`
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
              metrics.datetime.addresses_sync_finished_at
            ) AS addresses_sync_finished_at,
            metrics.datetime.addresses_sync_finished_at AS raw_addresses_sync_finished_at,
            mozfun.glean.parse_datetime(
              metrics.datetime.addresses_sync_started_at
            ) AS addresses_sync_started_at,
            metrics.datetime.addresses_sync_started_at AS raw_addresses_sync_started_at
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox_stable.addresses_sync_v1`
