CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.org_mozilla_fenix_derived.events_stream_live_v1`
  CLUSTER BY
    sample_id
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 30)
  AS
  SELECT
    * EXCEPT (metrics, events, name, category, extra, timestamp) REPLACE(
      STRUCT(
        client_info.app_build AS app_build,
        client_info.app_channel AS app_channel,
        client_info.app_display_version AS app_display_version,
        client_info.architecture AS architecture,
        client_info.device_manufacturer AS device_manufacturer,
        client_info.device_model AS device_model,
        client_info.first_run_date AS first_run_date,
        client_info.locale AS locale,
        client_info.os AS os,
        client_info.os_version AS os_version,
        client_info.telemetry_sdk_build AS telemetry_sdk_build,
        client_info.build_date AS build_date
      ) AS client_info,
      STRUCT(
        ping_info.seq,
        ping_info.start_time,
        ping_info.end_time,
        ping_info.ping_type
      ) AS ping_info
    ),
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    ping_info.reason AS reason,
    ping_info.experiments AS experiments,
    SAFE.TIMESTAMP_ADD(
      SAFE.PARSE_TIMESTAMP('%FT%H:%M%Ez', ping_info.start_time),
      INTERVAL event.timestamp MILLISECOND
    ) AS event_timestamp,
    (event.category || '.' || event.name) AS event,
    event.extra,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.events_v1` AS e
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    DATE(submission_timestamp) > '2023-11-01'
