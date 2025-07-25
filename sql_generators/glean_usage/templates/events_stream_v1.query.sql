{{ header }}
-- convert array of key value pairs to a json object, cast numbers and booleans if possible
CREATE TEMP FUNCTION from_map_event_extra(input ARRAY<STRUCT<key STRING, value STRING>>)
RETURNS json AS (
  IF(
    ARRAY_LENGTH(input) = 0,
    NULL,
    JSON_OBJECT(
      ARRAY(SELECT key FROM UNNEST(input)),
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
          UNNEST(input)
      )
    )
  )
);

-- convert array of key value pairs to a json object
-- values are nested structs and will be converted to json objects
CREATE TEMP FUNCTION from_map_experiment(
  input ARRAY<
    STRUCT<key STRING, value STRUCT<branch STRING, extra STRUCT<type STRING, enrollment_id STRING>>>
  >
)
RETURNS json AS (
  IF(
    ARRAY_LENGTH(input) = 0,
    NULL,
    JSON_OBJECT(ARRAY(SELECT key FROM UNNEST(input)), ARRAY(SELECT value FROM UNNEST(input)))
  )
);

CREATE TEMP FUNCTION metrics_to_json(metrics JSON)
RETURNS JSON AS (
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
  )
);

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
      ) AS ping_info
      {% if not metrics_as_struct and has_metrics %}
      ,
      metrics_to_json(TO_JSON(metrics)) AS metrics
      {% endif %}
    ),
    client_info.client_id AS client_id,
    ping_info.reason AS reason,
    from_map_experiment(ping_info.experiments) AS experiments,
    {% if has_profile_group_id %}
      metrics.uuid.legacy_telemetry_profile_group_id AS profile_group_id,
    {% else %}
      CAST(NULL AS STRING) AS profile_group_id,
    {% endif %}
    {% if has_legacy_telemetry_client_id %}
      metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
    {% else %}
      CAST(NULL AS STRING) AS legacy_telemetry_client_id,
    {% endif %}
  FROM
    `{{ events_view }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
    {% if slice_by_sample_id %}
      AND sample_id BETWEEN @min_sample_id AND @max_sample_id
    {% endif %}
)
--
SELECT
  base.* EXCEPT (events),
  COALESCE(
    SAFE.TIMESTAMP_MILLIS(SAFE_CAST(mozfun.map.get_key(event.extra, 'glean_timestamp') AS INT64)),
    SAFE.TIMESTAMP_ADD(ping_info.parsed_start_time, INTERVAL event.timestamp MILLISECOND)
  ) AS event_timestamp,
  event.category AS event_category,
  event.name AS event_name,
  ARRAY_TO_STRING([event.category, event.name], '.') AS event, -- handles NULL values better
  from_map_event_extra(event.extra) AS event_extra,
FROM
  base
CROSS JOIN
  UNNEST(events) AS event
  {% if app_name == "firefox_desktop" %}
    WHERE
      -- See https://mozilla-hub.atlassian.net/browse/DENG-7513
      NOT (
        normalized_channel = 'release'
        AND event.category = 'security'
        AND event.name = 'unexpected_load'
        AND app_version_major BETWEEN 132 AND 135
      )
      -- See https://bugzilla.mozilla.org/show_bug.cgi?id=1974286
      AND NOT (
        event.category = 'nimbus_events'
        AND event.name = 'enrollment_status'
        AND app_version_major = 140
      )
  {% elif app_name == "firefox_desktop_background_update" %}
    WHERE
      -- See https://mozilla-hub.atlassian.net/browse/DENG-8432
      NOT (
        app_version_major IN (138, 139)
          AND (
            (event.category = 'nimbus_events' AND event.name = 'validation_failed')
            OR (event.category = 'normandy' AND event.name = 'validation_failed_nimbus_experiment')
          )
      )
      -- See https://bugzilla.mozilla.org/show_bug.cgi?id=1974286
      AND NOT (
        event.category = 'nimbus_events'
        AND event.name = 'enrollment_status'
        AND app_version_major = 140
      )
  {% endif %}
