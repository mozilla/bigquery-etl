-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.heartbeat`
AS
SELECT
  * REPLACE (
    mozfun.norm.metadata(metadata) AS metadata,
    mozfun.norm.glean_ping_info(ping_info) AS ping_info,
    (
      SELECT AS STRUCT
        metrics.* REPLACE (
          STRUCT(
            mozfun.glean.parse_datetime(metrics.datetime.heartbeat_closed) AS heartbeat_closed,
            metrics.datetime.heartbeat_closed AS raw_heartbeat_closed,
            mozfun.glean.parse_datetime(metrics.datetime.heartbeat_engaged) AS heartbeat_engaged,
            metrics.datetime.heartbeat_engaged AS raw_heartbeat_engaged,
            mozfun.glean.parse_datetime(metrics.datetime.heartbeat_expired) AS heartbeat_expired,
            metrics.datetime.heartbeat_expired AS raw_heartbeat_expired,
            mozfun.glean.parse_datetime(
              metrics.datetime.heartbeat_learn_more
            ) AS heartbeat_learn_more,
            metrics.datetime.heartbeat_learn_more AS raw_heartbeat_learn_more,
            mozfun.glean.parse_datetime(metrics.datetime.heartbeat_offered) AS heartbeat_offered,
            metrics.datetime.heartbeat_offered AS raw_heartbeat_offered,
            mozfun.glean.parse_datetime(metrics.datetime.heartbeat_voted) AS heartbeat_voted,
            metrics.datetime.heartbeat_voted AS raw_heartbeat_voted,
            mozfun.glean.parse_datetime(
              metrics.datetime.heartbeat_window_closed
            ) AS heartbeat_window_closed,
            metrics.datetime.heartbeat_window_closed AS raw_heartbeat_window_closed
          ) AS datetime
        )
    ) AS metrics,
    'Firefox' AS normalized_app_name,
    mozfun.norm.glean_client_info_attribution(
      client_info,
      CAST(NULL AS JSON),
      CAST(NULL AS JSON)
    ) AS client_info
  ),
  mozfun.norm.extract_version(client_info.app_display_version, 'major') AS app_version_major,
  mozfun.norm.extract_version(client_info.app_display_version, 'minor') AS app_version_minor,
  mozfun.norm.extract_version(client_info.app_display_version, 'patch') AS app_version_patch,
  LOWER(IFNULL(metadata.isp.name, "")) = "browserstack" AS is_bot_generated,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.heartbeat_v1`
