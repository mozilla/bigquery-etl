{{ header }}

WITH base AS (
  SELECT
    * EXCEPT (metrics, events, name, category, extra, timestamp) REPLACE (
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
        ping_info.parsed_start_time,
        ping_info.end_time,
        ping_info.parsed_end_time,
        ping_info.ping_type
      ) AS ping_info
    ),
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    ping_info.reason AS reason,
    `mozfun.json.from_map`(ping_info.experiments) AS experiments,
    SAFE.TIMESTAMP_ADD(
      ping_info.parsed_start_time,
      INTERVAL event.timestamp MILLISECOND
    ) AS event_timestamp,
    event.category as event_category,
    event.name as event_name,
    ARRAY_TO_STRING([event.category, event.name], '.') AS event, -- handles NULL values better
    `mozfun.json.from_map`(event.extra) AS event_extra,
    JSON_STRIP_NULLS(
      TO_JSON(metrics),  -- expose as easy to access JSON column
      remove_empty => true -- no reason to keep empty objects around
    ) AS metrics
  FROM
    `{{ events_view }}` AS e
  CROSS JOIN
    UNNEST(events) AS event
  WHERE
    {% raw %}
    {% if is_init() %}
      DATE(submission_timestamp) >= '2023-11-01'
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    {% endraw %}
)
--
SELECT
  *
FROM
  base
