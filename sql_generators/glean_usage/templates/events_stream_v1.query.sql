{{ header }}
WITH base AS (
  SELECT
    * REPLACE (
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
        client_info.build_date AS build_date,
        client_info.session_id AS session_id,
        client_info.session_count AS session_count
      ) AS client_info,
      STRUCT(
        ping_info.seq,
        ping_info.start_time,
        ping_info.parsed_start_time,
        ping_info.end_time,
        ping_info.parsed_end_time,
        ping_info.ping_type
      ) AS ping_info,
      TO_JSON(metrics) AS metrics
    ),
    client_info.client_id AS client_id,
    ping_info.reason AS reason,
    -- convert array of key value pairs to a json object
    -- values are nested structs and will be converted to json objects
    IF(
      ARRAY_LENGTH(ping_info.experiments) = 0,
      NULL,
      JSON_OBJECT(
        ARRAY(SELECT key FROM UNNEST(ping_info.experiments)),
        ARRAY(SELECT value FROM UNNEST(ping_info.experiments))
      )
    ) AS experiments,
  FROM
    `{{ events_view }}`
  WHERE
    {% raw %}
    {% if is_init() %}
      DATE(submission_timestamp) >= '2023-11-01'
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    {% endraw %}
),
json_metrics AS (
  SELECT
    * REPLACE (
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
)
--
SELECT
  json_metrics.* EXCEPT (events),
  COALESCE(
    SAFE.TIMESTAMP_MILLIS(SAFE_CAST(mozfun.map.get_key(event.extra, 'glean_timestamp') AS INT64)),
    SAFE.TIMESTAMP_ADD(ping_info.parsed_start_time, INTERVAL event.timestamp MILLISECOND)
  ) AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  ARRAY_TO_STRING([event.category, event.name], '.') AS event, -- handles NULL values better
  -- convert array of key value pairs to a json object, cast numbers and booleans if possible
  IF(
    ARRAY_LENGTH(event.extra) = 0,
    NULL,
    JSON_OBJECT(
      ARRAY(SELECT key FROM UNNEST(event.extra)),
      ARRAY(
        SELECT
          CASE
            WHEN SAFE_CAST(value AS NUMERIC) IS NOT NULL
              THEN TO_JSON(SAFE_CAST(value AS NUMERIC))
            WHEN SAFE_CAST(value AS BOOL) IS NOT NULL
              THEN TO_JSON(SAFE_CAST(value AS BOOL))
            ELSE TO_JSON(value)
          END
        FROM
          UNNEST(event.extra)
      )
    )
  ) AS event_extra,
FROM
  json_metrics
CROSS JOIN
  UNNEST(events) AS event
