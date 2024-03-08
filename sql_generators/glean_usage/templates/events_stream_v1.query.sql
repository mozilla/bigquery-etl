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
    TO_JSON(metrics) AS metrics
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
  * REPLACE (
    -- expose as easy to access JSON column,
    -- strip nulls,
    -- translate nested array records into a JSON object,
    -- rename url2 -> url
    JSON_STRIP_NULLS(
      JSON_REMOVE(
        -- labeled_* are the only ones that SHOULD show up as context for events pings,
        -- thus we special-case them
        --
        -- The JSON_SET/JSON_EXTRACT shenanigans are needed
        -- because those subfields might not exist, so accessing the columns would fail.
        -- but accessing non-existent fields in a JSON object simply gives us NULL.
        JSON_SET(
          metrics,
          '$.labeled_counter',
          mozfun.json.from_nested_map(metrics.labeled_counter),
          '$.labeled_string',
          mozfun.json.from_nested_map(metrics.labeled_string),
          '$.labeled_boolean',
          mozfun.json.from_nested_map(metrics.labeled_boolean),
          '$.url',
          metrics.url2
        ),
        '$.url2'
      ),
      remove_empty => TRUE
    ) AS metrics
  )
FROM
  base
