-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser.history_sync`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* EXCEPT (jwe, labeled_rate, text, url) REPLACE(
          STRUCT(
            mozfun.glean.parse_datetime(
              metrics.datetime.history_sync_finished_at
            ) AS history_sync_finished_at,
            metrics.datetime.history_sync_finished_at AS raw_history_sync_finished_at,
            mozfun.glean.parse_datetime(
              metrics.datetime.history_sync_started_at
            ) AS history_sync_started_at,
            metrics.datetime.history_sync_started_at AS raw_history_sync_started_at,
            mozfun.glean.parse_datetime(
              metrics.datetime.history_sync_v2_finished_at
            ) AS history_sync_v2_finished_at,
            metrics.datetime.history_sync_v2_finished_at AS raw_history_sync_v2_finished_at,
            mozfun.glean.parse_datetime(
              metrics.datetime.history_sync_v2_started_at
            ) AS history_sync_v2_started_at,
            metrics.datetime.history_sync_v2_started_at AS raw_history_sync_v2_started_at
          ) AS datetime
        )
    ) AS metrics
  )
FROM
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser_stable.history_sync_v1`