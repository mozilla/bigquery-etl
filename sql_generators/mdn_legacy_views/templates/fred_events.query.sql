{#- Fred events reshaped to the Yari ping schema. Only fields that Fred provides
are listed here, the generator sets all other Yari fields to NULL. #}
SELECT
  '{}' AS additional_properties,
  STRUCT(
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
    client_info.session_id AS session_id
  ) AS client_info,
  {% if event_name == 'element_click' %}
  [
    STRUCT(
      'element' AS category,
      [
        STRUCT(
          'source' AS `key`,
          CASE
            WHEN JSON_VALUE(event_extra.label) IS NOT NULL
              AND JSON_VALUE(event_extra.label) != ''
              AND JSON_VALUE(event_extra.type) IS NOT NULL
              AND JSON_VALUE(event_extra.type) != ''
              THEN CONCAT(
                  JSON_VALUE(event_extra.id),
                  ': ',
                  JSON_VALUE(event_extra.type),
                  ' -> ',
                  JSON_VALUE(event_extra.label)
                )
            WHEN JSON_VALUE(event_extra.label) IS NOT NULL
              AND JSON_VALUE(event_extra.label) != ''
              THEN CONCAT(JSON_VALUE(event_extra.id), ': ', JSON_VALUE(event_extra.label))
            WHEN JSON_VALUE(event_extra.type) IS NOT NULL
              AND JSON_VALUE(event_extra.type) != ''
              THEN CONCAT(JSON_VALUE(event_extra.id), ': ', JSON_VALUE(event_extra.type))
            ELSE JSON_VALUE(event_extra.id)
          END AS `value`
        ),
        STRUCT('subscription_type' AS `key`, CAST(NULL AS STRING) AS `value`),
        STRUCT('glean_timestamp' AS `key`, TO_JSON_STRING(UNIX_MICROS(event_timestamp)) AS `value`)
      ] AS extra,
      'clicked' AS name,
      0 AS `timestamp`
    )
  ] AS events,
  {% endif %}
  metadata,
  STRUCT(
    STRUCT(
      IFNULL(JSON_VALUE(event_extra.url), '') AS page_path,
      IFNULL(JSON_VALUE(event_extra.referrer), '') AS page_referrer
    ) AS url2,
    STRUCT(
      IFNULL(country_codes_v1.name, 'Unknown') AS navigator_geo,
      'unknown' AS navigator_subscription_type,
      IFNULL(metadata.geo.country, '??') AS navigator_geo_iso
    ) AS `string`,
    STRUCT(
      ARRAY(
        SELECT AS STRUCT
          key,
          value
        FROM
          (
            SELECT
              REGEXP_EXTRACT(kv, r'^utm_(.*?)=') AS key,
              REGEXP_EXTRACT(kv, r'=(.*)$') AS value,
              off
            FROM
              UNNEST(REGEXP_EXTRACT_ALL(JSON_VALUE(event_extra.url), r'[?&](utm_[^&]+)')) AS kv
              WITH OFFSET off
          )
        QUALIFY
          ROW_NUMBER() OVER (PARTITION BY key ORDER BY off DESC) = 1
      ) AS page_utm
    ) AS labeled_string,
    STRUCT(
      IFNULL(JSON_VALUE(event_extra.url), '') AS page_path,
      IFNULL(JSON_VALUE(event_extra.referrer), '') AS page_referrer
    ) AS url
  ) AS metrics,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  STRUCT(
    ping_info.end_time AS end_time,
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
  `{{ project_id }}.mdn_fred.events_stream`
LEFT JOIN
  `{{ project_id }}.static.country_codes_v1` country_codes_v1
  ON country_codes_v1.code = metadata.geo.country
WHERE
  event_category = 'glean'
  AND event_name = '{{ event_name }}'
