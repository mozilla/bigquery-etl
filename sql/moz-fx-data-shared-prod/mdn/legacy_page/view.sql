CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mdn.legacy_page` AS (
    SELECT
      '{}' AS additional_properties,
      STRUCT(
        CAST(NULL AS STRING) AS android_sdk_version,
        client_info.app_build AS app_build,
        client_info.app_channel AS app_channel,
        client_info.app_display_version AS app_display_version,
        client_info.architecture AS architecture,
        client_info.build_date AS build_date,
        client_id AS client_id,
        client_info.device_manufacturer AS device_manufacturer,
        client_info.device_model AS device_model,
        client_info.first_run_date AS first_run_date,
        client_info.locale AS locale,
        client_info.os AS os,
        client_info.os_version AS os_version,
        client_info.telemetry_sdk_build AS telemetry_sdk_build,
        CAST(NULL AS INT64) AS windows_build_number,
        CAST(NULL AS INT64) AS session_count,
        client_info.session_id AS session_id,
        STRUCT(
          CAST(NULL AS STRING) AS campaign,
          CAST(NULL AS STRING) AS content,
          CAST(NULL AS STRING) AS medium,
          CAST(NULL AS STRING) AS source,
          CAST(NULL AS STRING) AS term,
          CAST(NULL AS JSON) AS ext
        ) AS attribution,
        STRUCT(CAST(NULL AS STRING) AS name, CAST(NULL AS JSON) AS ext) AS distribution
      ) AS client_info,
      CAST(NULL AS STRING) AS document_id,
      CAST(
        []
        AS
          ARRAY<
            STRUCT<
              source STRING,
              extra ARRAY<STRUCT<key STRING, value STRING>>,
              name STRING,
              `timestamp` INTEGER
            >
          >
      ) AS events,
      metadata,
      STRUCT(
        CAST(
          NULL
          AS
            STRUCT<
              glean_error_invalid_label ARRAY<STRUCT<key STRING, value INT64>>,
              glean_error_invalid_overflow ARRAY<STRUCT<key STRING, value INT64>>,
              glean_error_invalid_state ARRAY<STRUCT<key STRING, value INT64>>,
              glean_error_invalid_value ARRAY<STRUCT<key STRING, value INT64>>
            >
        ) AS labeled_counter,
        STRUCT(
          JSON_VALUE(event_extra.url) AS page_path,
          JSON_VALUE(event_extra.referrer) AS page_referrer
        ) AS url2,
        STRUCT(
          metadata.geo.country AS navigator_geo,
          CAST(NULL AS STRING) AS navigator_subscription_type,
          CAST(NULL AS STRING) AS navigator_user_agent,
          CAST(NULL AS STRING) AS navigator_viewport_breakpoint,
          CAST(NULL AS STRING) AS page_http_status,
          CAST(NULL AS STRING) AS page_is_baseline,
          CAST(NULL AS STRING) AS navigator_geo_iso,
          CAST(NULL AS STRING) AS glean_client_annotation_experimentation_id
        ) AS `string`,
        STRUCT(
          CAST(NULL AS INTEGER) AS navigator_viewport_horizontal_coverage,
          CAST(NULL AS INTEGER) AS navigator_viewport_ratio
        ) AS `quantity`,
        STRUCT(
          ARRAY(
            SELECT AS STRUCT
              REGEXP_EXTRACT(kv, r'^utm_(.*?)=') AS key,
              REGEXP_EXTRACT(kv, r'=(.*)$') AS value
            FROM
              UNNEST(REGEXP_EXTRACT_ALL(JSON_VALUE(event_extra.url), r'[?&](utm_[^&]+)')) AS kv
          ) AS page_utm
        ) AS labeled_string,
        STRUCT(CAST(NULL AS ARRAY<STRING>) AS navigator_user_languages) AS string_list,
        STRUCT(
          JSON_VALUE(event_extra.url) AS page_path,
          JSON_VALUE(event_extra.referrer) AS page_referrer
        ) AS url
      ) AS metrics,
      normalized_app_name,
      normalized_channel,
      normalized_country_code,
      normalized_os,
      normalized_os_version,
      STRUCT(
        ping_info.end_time AS end_time,
        CAST(
          []
          AS
            ARRAY<
              STRUCT<
                key STRING,
                value STRUCT<branch STRING, extra STRUCT<type STRING, enrollment_id STRING>>
              >
            >
        ) AS experiments,
        ping_info.ping_type AS ping_type,
        reason AS reason,
        ping_info.seq AS seq,
        ping_info.start_time AS start_time,
        ping_info.parsed_start_time AS parsed_start_time,
        ping_info.parsed_end_time AS parsed_end_time
      ) AS ping_info,
      sample_id,
      submission_timestamp,
      app_version_major,
      app_version_minor,
      app_version_patch,
      is_bot_generated
    FROM
      `moz-fx-data-shared-prod.mdn_fred.events_stream`
    WHERE
      event_category = 'glean'
      AND event_name = 'page_load'
    UNION ALL
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.mdn_yari.page`
  )
