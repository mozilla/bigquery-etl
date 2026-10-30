-- Generated via ./bqetl generate mdn_legacy_views
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mdn.legacy_action`
AS
WITH fred AS (
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
          STRUCT(
            'glean_timestamp' AS `key`,
            TO_JSON_STRING(UNIX_MICROS(event_timestamp)) AS `value`
          )
        ] AS extra,
        'clicked' AS name,
        0 AS `timestamp`
      )
    ] AS events,
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
    `moz-fx-data-shared-prod.mdn_fred.events_stream`
  LEFT JOIN
    `moz-fx-data-shared-prod.static.country_codes_v1` country_codes_v1
    ON country_codes_v1.code = metadata.geo.country
  WHERE
    event_category = 'glean'
    AND event_name = 'element_click'
)
SELECT
  additional_properties,
  IF(
    client_info IS NULL,
    NULL,
    STRUCT(
      CAST(NULL AS STRING) AS `android_sdk_version`,
      client_info.app_build,
      client_info.app_channel,
      client_info.app_display_version,
      client_info.architecture,
      client_info.build_date,
      client_info.client_id,
      client_info.device_manufacturer,
      client_info.device_model,
      client_info.first_run_date,
      client_info.locale,
      client_info.os,
      client_info.os_version,
      client_info.telemetry_sdk_build,
      CAST(NULL AS INTEGER) AS `windows_build_number`,
      CAST(NULL AS INTEGER) AS `session_count`,
      client_info.session_id,
      CAST(
        NULL
        AS
          STRUCT<
            `campaign` STRING,
            `content` STRING,
            `medium` STRING,
            `source` STRING,
            `term` STRING,
            `ext` JSON
          >
      ) AS `attribution`,
      CAST(NULL AS STRUCT<`name` STRING, `ext` JSON>) AS `distribution`
    )
  ) AS `client_info`,
  CAST(NULL AS STRING) AS `document_id`,
  ARRAY(
    SELECT
      STRUCT(
        events.category,
        ARRAY(
          SELECT
            STRUCT(extra.key, extra.value)
          FROM
            UNNEST(events.extra) AS `extra`
        ) AS `extra`,
        events.name,
        events.timestamp,
        CAST(
          NULL
          AS
            STRUCT<
              `event_seq` INTEGER,
              `session_id` STRING,
              `session_sample_rate` FLOAT64,
              `session_seq` INTEGER,
              `session_start_time` STRING
            >
        ) AS `session`
      )
    FROM
      UNNEST(events) AS `events`
  ) AS `events`,
  IF(
    metadata IS NULL,
    NULL,
    STRUCT(
      IF(
        metadata.geo IS NULL,
        NULL,
        STRUCT(
          metadata.geo.city,
          metadata.geo.country,
          metadata.geo.db_version,
          metadata.geo.subdivision1,
          metadata.geo.subdivision2
        )
      ) AS `geo`,
      IF(
        metadata.header IS NULL,
        NULL,
        STRUCT(
          metadata.header.date,
          metadata.header.dnt,
          metadata.header.x_debug_id,
          metadata.header.x_foxsec_ip_reputation,
          metadata.header.x_lb_tags,
          metadata.header.x_pingsender_version,
          metadata.header.x_source_tags,
          metadata.header.x_telemetry_agent,
          metadata.header.parsed_date,
          metadata.header.parsed_x_source_tags,
          IF(
            metadata.header.parsed_x_lb_tags IS NULL,
            NULL,
            STRUCT(
              metadata.header.parsed_x_lb_tags.tls_version,
              metadata.header.parsed_x_lb_tags.tls_cipher_hex
            )
          ) AS `parsed_x_lb_tags`
        )
      ) AS `header`,
      IF(
        metadata.isp IS NULL,
        NULL,
        STRUCT(metadata.isp.db_version, metadata.isp.name, metadata.isp.organization)
      ) AS `isp`,
      IF(
        metadata.user_agent IS NULL,
        NULL,
        STRUCT(metadata.user_agent.browser, metadata.user_agent.os, metadata.user_agent.version)
      ) AS `user_agent`
    )
  ) AS `metadata`,
  IF(
    metrics IS NULL,
    NULL,
    STRUCT(
      CAST(
        NULL
        AS
          STRUCT<
            `glean_error_invalid_label` ARRAY<STRUCT<`key` STRING, `value` INTEGER>>,
            `glean_error_invalid_overflow` ARRAY<STRUCT<`key` STRING, `value` INTEGER>>,
            `glean_error_invalid_state` ARRAY<STRUCT<`key` STRING, `value` INTEGER>>,
            `glean_error_invalid_value` ARRAY<STRUCT<`key` STRING, `value` INTEGER>>,
            `glean_error_invalid_type` ARRAY<STRUCT<`key` STRING, `value` INTEGER>>
          >
      ) AS `labeled_counter`,
      IF(
        metrics.url2 IS NULL,
        NULL,
        STRUCT(metrics.url2.page_path, metrics.url2.page_referrer)
      ) AS `url2`,
      IF(
        metrics.string IS NULL,
        NULL,
        STRUCT(
          metrics.string.navigator_geo,
          metrics.string.navigator_subscription_type,
          CAST(NULL AS STRING) AS `navigator_user_agent`,
          CAST(NULL AS STRING) AS `navigator_viewport_breakpoint`,
          CAST(NULL AS STRING) AS `page_http_status`,
          CAST(NULL AS STRING) AS `page_is_baseline`,
          metrics.string.navigator_geo_iso,
          CAST(NULL AS STRING) AS `glean_client_annotation_experimentation_id`
        )
      ) AS `string`,
      CAST(
        NULL
        AS
          STRUCT<
            `navigator_viewport_horizontal_coverage` INTEGER,
            `navigator_viewport_ratio` INTEGER
          >
      ) AS `quantity`,
      IF(
        metrics.labeled_string IS NULL,
        NULL,
        STRUCT(
          ARRAY(
            SELECT
              STRUCT(page_utm.key, page_utm.value)
            FROM
              UNNEST(metrics.labeled_string.page_utm) AS `page_utm`
          ) AS `page_utm`
        )
      ) AS `labeled_string`,
      CAST(NULL AS STRUCT<`navigator_user_languages` ARRAY<STRING>>) AS `string_list`,
      IF(
        metrics.url IS NULL,
        NULL,
        STRUCT(metrics.url.page_path, metrics.url.page_referrer)
      ) AS `url`
    )
  ) AS `metrics`,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  IF(
    ping_info IS NULL,
    NULL,
    STRUCT(
      ping_info.end_time,
      CAST(
        []
        AS
          ARRAY<
            STRUCT<
              `key` STRING,
              `value` STRUCT<`branch` STRING, `extra` STRUCT<`type` STRING, `enrollment_id` STRING>>
            >
          >
      ) AS `experiments`,
      ping_info.ping_type,
      ping_info.reason,
      ping_info.seq,
      ping_info.start_time,
      CAST(
        NULL
        AS
          STRUCT<
            `event_threshold` INTEGER,
            `metrics_enabled` ARRAY<STRUCT<`key` STRING, `value` BOOLEAN>>,
            `pings_enabled` ARRAY<STRUCT<`key` STRING, `value` BOOLEAN>>,
            `session_sample_rate` FLOAT64,
            `events_ping_acceleration_factor` FLOAT64
          >
      ) AS `server_knobs_config`,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    )
  ) AS `ping_info`,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  fred
UNION ALL
SELECT
  additional_properties,
  IF(
    client_info IS NULL,
    NULL,
    STRUCT(
      client_info.android_sdk_version,
      client_info.app_build,
      client_info.app_channel,
      client_info.app_display_version,
      client_info.architecture,
      client_info.build_date,
      client_info.client_id,
      client_info.device_manufacturer,
      client_info.device_model,
      client_info.first_run_date,
      client_info.locale,
      client_info.os,
      client_info.os_version,
      client_info.telemetry_sdk_build,
      client_info.windows_build_number,
      client_info.session_count,
      client_info.session_id,
      IF(
        client_info.attribution IS NULL,
        NULL,
        STRUCT(
          client_info.attribution.campaign,
          client_info.attribution.content,
          client_info.attribution.medium,
          client_info.attribution.source,
          client_info.attribution.term,
          client_info.attribution.ext
        )
      ) AS `attribution`,
      IF(
        client_info.distribution IS NULL,
        NULL,
        STRUCT(client_info.distribution.name, client_info.distribution.ext)
      ) AS `distribution`
    )
  ) AS `client_info`,
  document_id,
  ARRAY(
    SELECT
      STRUCT(
        events.category,
        ARRAY(
          SELECT
            STRUCT(extra.key, extra.value)
          FROM
            UNNEST(events.extra) AS `extra`
        ) AS `extra`,
        events.name,
        events.timestamp,
        IF(
          events.session IS NULL,
          NULL,
          STRUCT(
            events.session.event_seq,
            events.session.session_id,
            events.session.session_sample_rate,
            events.session.session_seq,
            events.session.session_start_time
          )
        ) AS `session`
      )
    FROM
      UNNEST(events) AS `events`
  ) AS `events`,
  IF(
    metadata IS NULL,
    NULL,
    STRUCT(
      IF(
        metadata.geo IS NULL,
        NULL,
        STRUCT(
          metadata.geo.city,
          metadata.geo.country,
          metadata.geo.db_version,
          metadata.geo.subdivision1,
          metadata.geo.subdivision2
        )
      ) AS `geo`,
      IF(
        metadata.header IS NULL,
        NULL,
        STRUCT(
          metadata.header.date,
          metadata.header.dnt,
          metadata.header.x_debug_id,
          metadata.header.x_foxsec_ip_reputation,
          metadata.header.x_lb_tags,
          metadata.header.x_pingsender_version,
          metadata.header.x_source_tags,
          metadata.header.x_telemetry_agent,
          metadata.header.parsed_date,
          metadata.header.parsed_x_source_tags,
          IF(
            metadata.header.parsed_x_lb_tags IS NULL,
            NULL,
            STRUCT(
              metadata.header.parsed_x_lb_tags.tls_version,
              metadata.header.parsed_x_lb_tags.tls_cipher_hex
            )
          ) AS `parsed_x_lb_tags`
        )
      ) AS `header`,
      IF(
        metadata.isp IS NULL,
        NULL,
        STRUCT(metadata.isp.db_version, metadata.isp.name, metadata.isp.organization)
      ) AS `isp`,
      IF(
        metadata.user_agent IS NULL,
        NULL,
        STRUCT(metadata.user_agent.browser, metadata.user_agent.os, metadata.user_agent.version)
      ) AS `user_agent`
    )
  ) AS `metadata`,
  IF(
    metrics IS NULL,
    NULL,
    STRUCT(
      IF(
        metrics.labeled_counter IS NULL,
        NULL,
        STRUCT(
          ARRAY(
            SELECT
              STRUCT(glean_error_invalid_label.key, glean_error_invalid_label.value)
            FROM
              UNNEST(
                metrics.labeled_counter.glean_error_invalid_label
              ) AS `glean_error_invalid_label`
          ) AS `glean_error_invalid_label`,
          ARRAY(
            SELECT
              STRUCT(glean_error_invalid_overflow.key, glean_error_invalid_overflow.value)
            FROM
              UNNEST(
                metrics.labeled_counter.glean_error_invalid_overflow
              ) AS `glean_error_invalid_overflow`
          ) AS `glean_error_invalid_overflow`,
          ARRAY(
            SELECT
              STRUCT(glean_error_invalid_state.key, glean_error_invalid_state.value)
            FROM
              UNNEST(
                metrics.labeled_counter.glean_error_invalid_state
              ) AS `glean_error_invalid_state`
          ) AS `glean_error_invalid_state`,
          ARRAY(
            SELECT
              STRUCT(glean_error_invalid_value.key, glean_error_invalid_value.value)
            FROM
              UNNEST(
                metrics.labeled_counter.glean_error_invalid_value
              ) AS `glean_error_invalid_value`
          ) AS `glean_error_invalid_value`,
          ARRAY(
            SELECT
              STRUCT(glean_error_invalid_type.key, glean_error_invalid_type.value)
            FROM
              UNNEST(metrics.labeled_counter.glean_error_invalid_type) AS `glean_error_invalid_type`
          ) AS `glean_error_invalid_type`
        )
      ) AS `labeled_counter`,
      IF(
        metrics.url2 IS NULL,
        NULL,
        STRUCT(metrics.url2.page_path, metrics.url2.page_referrer)
      ) AS `url2`,
      IF(
        metrics.string IS NULL,
        NULL,
        STRUCT(
          metrics.string.navigator_geo,
          metrics.string.navigator_subscription_type,
          metrics.string.navigator_user_agent,
          metrics.string.navigator_viewport_breakpoint,
          metrics.string.page_http_status,
          metrics.string.page_is_baseline,
          metrics.string.navigator_geo_iso,
          metrics.string.glean_client_annotation_experimentation_id
        )
      ) AS `string`,
      IF(
        metrics.quantity IS NULL,
        NULL,
        STRUCT(
          metrics.quantity.navigator_viewport_horizontal_coverage,
          metrics.quantity.navigator_viewport_ratio
        )
      ) AS `quantity`,
      IF(
        metrics.labeled_string IS NULL,
        NULL,
        STRUCT(
          ARRAY(
            SELECT
              STRUCT(page_utm.key, page_utm.value)
            FROM
              UNNEST(metrics.labeled_string.page_utm) AS `page_utm`
          ) AS `page_utm`
        )
      ) AS `labeled_string`,
      IF(
        metrics.string_list IS NULL,
        NULL,
        STRUCT(metrics.string_list.navigator_user_languages)
      ) AS `string_list`,
      IF(
        metrics.url IS NULL,
        NULL,
        STRUCT(metrics.url.page_path, metrics.url.page_referrer)
      ) AS `url`
    )
  ) AS `metrics`,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  IF(
    ping_info IS NULL,
    NULL,
    STRUCT(
      ping_info.end_time,
      ARRAY(
        SELECT
          STRUCT(
            experiments.key,
            IF(
              experiments.value IS NULL,
              NULL,
              STRUCT(
                experiments.value.branch,
                IF(
                  experiments.value.extra IS NULL,
                  NULL,
                  STRUCT(experiments.value.extra.type, experiments.value.extra.enrollment_id)
                ) AS `extra`
              )
            ) AS `value`
          )
        FROM
          UNNEST(ping_info.experiments) AS `experiments`
      ) AS `experiments`,
      ping_info.ping_type,
      ping_info.reason,
      ping_info.seq,
      ping_info.start_time,
      IF(
        ping_info.server_knobs_config IS NULL,
        NULL,
        STRUCT(
          ping_info.server_knobs_config.event_threshold,
          ARRAY(
            SELECT
              STRUCT(metrics_enabled.key, metrics_enabled.value)
            FROM
              UNNEST(ping_info.server_knobs_config.metrics_enabled) AS `metrics_enabled`
          ) AS `metrics_enabled`,
          ARRAY(
            SELECT
              STRUCT(pings_enabled.key, pings_enabled.value)
            FROM
              UNNEST(ping_info.server_knobs_config.pings_enabled) AS `pings_enabled`
          ) AS `pings_enabled`,
          ping_info.server_knobs_config.session_sample_rate,
          ping_info.server_knobs_config.events_ping_acceleration_factor
        )
      ) AS `server_knobs_config`,
      ping_info.parsed_start_time,
      ping_info.parsed_end_time
    )
  ) AS `ping_info`,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.mdn_yari.action`
