-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.gecko_trace`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.gecko_trace`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.gecko_trace`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  STRUCT(
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    STRUCT(
      client_info.attribution.campaign,
      client_info.attribution.content,
      client_info.attribution.medium,
      client_info.attribution.source,
      client_info.attribution.term,
      client_info.attribution.ext
    ) AS `attribution`,
    client_info.build_date,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    STRUCT(client_info.distribution.name, client_info.distribution.ext) AS `distribution`,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.session_count,
    client_info.session_id,
    client_info.telemetry_sdk_build,
    client_info.windows_build_number
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
        events.timestamp
      )
    FROM
      UNNEST(events) AS `events`
  ) AS `events`,
  STRUCT(
    STRUCT(
      metadata.geo.city,
      metadata.geo.country,
      metadata.geo.db_version,
      metadata.geo.subdivision1,
      metadata.geo.subdivision2
    ) AS `geo`,
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
      STRUCT(
        metadata.header.parsed_x_lb_tags.tls_version,
        metadata.header.parsed_x_lb_tags.tls_cipher_hex
      ) AS `parsed_x_lb_tags`
    ) AS `header`,
    STRUCT(metadata.isp.db_version, metadata.isp.name, metadata.isp.organization) AS `isp`,
    STRUCT(
      metadata.user_agent.browser,
      metadata.user_agent.os,
      metadata.user_agent.version
    ) AS `user_agent`
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      ARRAY(
        SELECT
          STRUCT(glean_error_invalid_label.key, glean_error_invalid_label.value)
        FROM
          UNNEST(metrics.labeled_counter.glean_error_invalid_label) AS `glean_error_invalid_label`
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
          UNNEST(metrics.labeled_counter.glean_error_invalid_state) AS `glean_error_invalid_state`
      ) AS `glean_error_invalid_state`,
      ARRAY(
        SELECT
          STRUCT(glean_error_invalid_value.key, glean_error_invalid_value.value)
        FROM
          UNNEST(metrics.labeled_counter.glean_error_invalid_value) AS `glean_error_invalid_value`
      ) AS `glean_error_invalid_value`
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  STRUCT(
    ping_info.end_time,
    ARRAY(
      SELECT
        STRUCT(
          experiments.key,
          STRUCT(
            experiments.value.branch,
            STRUCT(experiments.value.extra.enrollment_id, experiments.value.extra.type) AS `extra`
          ) AS `value`
        )
      FROM
        UNNEST(ping_info.experiments) AS `experiments`
    ) AS `experiments`,
    ping_info.ping_type,
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    ping_info.parsed_start_time,
    ping_info.parsed_end_time
  ) AS `ping_info`,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.gecko_trace`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.gecko_trace`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.gecko_trace`
