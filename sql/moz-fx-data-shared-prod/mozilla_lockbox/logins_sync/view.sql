-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_lockbox.logins_sync`
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
              metrics.datetime.logins_sync_finished_at
            ) AS logins_sync_finished_at,
            metrics.datetime.logins_sync_finished_at AS raw_logins_sync_finished_at,
            mozfun.glean.parse_datetime(
              metrics.datetime.logins_sync_started_at
            ) AS logins_sync_started_at,
            metrics.datetime.logins_sync_started_at AS raw_logins_sync_started_at,
            mozfun.glean.parse_datetime(
              metrics.datetime.logins_sync_v2_finished_at
            ) AS logins_sync_v2_finished_at,
            metrics.datetime.logins_sync_v2_finished_at AS raw_logins_sync_v2_finished_at,
            mozfun.glean.parse_datetime(
              metrics.datetime.logins_sync_v2_started_at
            ) AS logins_sync_v2_started_at,
            metrics.datetime.logins_sync_v2_started_at AS raw_logins_sync_v2_started_at
          ) AS datetime
        )
    ) AS metrics,
    mozfun.norm.glean_client_info_attribution(
      client_info,
      CAST(NULL AS JSON),
      CAST(NULL AS JSON)
    ) AS client_info
  ),
  mozfun.norm.extract_version(client_info.app_display_version, 'major') AS app_version_major,
  mozfun.norm.extract_version(client_info.app_display_version, 'minor') AS app_version_minor,
  mozfun.norm.extract_version(client_info.app_display_version, 'patch') AS app_version_patch
FROM
  `moz-fx-data-shared-prod.mozilla_lockbox_stable.logins_sync_v1`
