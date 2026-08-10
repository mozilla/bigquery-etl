-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.first_session`
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
      metrics.datetime.first_session_timestamp,
      metrics.datetime.raw_first_session_timestamp
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.first_session_adgroup,
      metrics.string.first_session_campaign,
      metrics.string.first_session_creative,
      metrics.string.first_session_network,
      metrics.string.first_session_distribution_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.meta_attribution_app,
      metrics.string.meta_attribution_nonce,
      metrics.string.meta_attribution_t,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.first_session_adjust_attribution_time
    ) AS `timing_distribution`,
    STRUCT(metrics.timespan.first_session_adjust_attribution_timespan) AS `timespan`,
    STRUCT(
      metrics.text2.meta_attribution_data,
      metrics.text2.play_store_attribution_install_referrer_response
    ) AS `text2`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.text.meta_attribution_data,
      metrics.text.play_store_attribution_install_referrer_response
    ) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.first_session`
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
      metrics.datetime.first_session_timestamp,
      metrics.datetime.raw_first_session_timestamp
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.first_session_adgroup,
      metrics.string.first_session_campaign,
      metrics.string.first_session_creative,
      metrics.string.first_session_network,
      metrics.string.first_session_distribution_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.meta_attribution_app,
      metrics.string.meta_attribution_nonce,
      metrics.string.meta_attribution_t,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.first_session_adjust_attribution_time
    ) AS `timing_distribution`,
    STRUCT(metrics.timespan.first_session_adjust_attribution_timespan) AS `timespan`,
    STRUCT(
      metrics.text2.meta_attribution_data,
      metrics.text2.play_store_attribution_install_referrer_response
    ) AS `text2`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.text.meta_attribution_data,
      metrics.text.play_store_attribution_install_referrer_response
    ) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.first_session`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.datetime.first_session_timestamp,
      metrics.datetime.raw_first_session_timestamp
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.first_session_adgroup,
      metrics.string.first_session_campaign,
      metrics.string.first_session_creative,
      metrics.string.first_session_network,
      metrics.string.first_session_distribution_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.meta_attribution_app,
      metrics.string.meta_attribution_nonce,
      metrics.string.meta_attribution_t,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.first_session_adjust_attribution_time
    ) AS `timing_distribution`,
    STRUCT(metrics.timespan.first_session_adjust_attribution_timespan) AS `timespan`,
    STRUCT(
      metrics.text2.meta_attribution_data,
      metrics.text2.play_store_attribution_install_referrer_response
    ) AS `text2`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.text.meta_attribution_data,
      metrics.text.play_store_attribution_install_referrer_response
    ) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.first_session`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  STRUCT(
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.telemetry_sdk_build,
    client_info.build_date,
    client_info.windows_build_number,
    client_info.session_count,
    client_info.session_id,
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
  document_id,
  events,
  STRUCT(
    metadata.geo,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      metadata.header.parsed_date,
      metadata.header.parsed_x_source_tags,
      metadata.header.parsed_x_lb_tags
    ) AS `header`,
    metadata.isp,
    metadata.user_agent
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.datetime.first_session_timestamp,
      metrics.datetime.raw_first_session_timestamp
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.first_session_adgroup,
      metrics.string.first_session_campaign,
      metrics.string.first_session_creative,
      metrics.string.first_session_network,
      metrics.string.first_session_distribution_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.meta_attribution_app,
      metrics.string.meta_attribution_nonce,
      metrics.string.meta_attribution_t,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(
      STRUCT(
        CAST(NULL AS INTEGER) AS `bucket_count`,
        CAST(NULL AS INTEGER) AS `count`,
        CAST(NULL AS STRING) AS `histogram_type`,
        CAST(NULL AS INTEGER) AS `overflow`,
        CAST(NULL AS ARRAY<FLOAT64>) AS `range`,
        metrics.timing_distribution.first_session_adjust_attribution_time.sum,
        CAST(NULL AS STRING) AS `time_unit`,
        CAST(NULL AS INTEGER) AS `underflow`,
        metrics.timing_distribution.first_session_adjust_attribution_time.values
      ) AS `first_session_adjust_attribution_time`
    ) AS `timing_distribution`,
    STRUCT(metrics.timespan.first_session_adjust_attribution_timespan) AS `timespan`,
    STRUCT(
      metrics.text2.meta_attribution_data,
      metrics.text2.play_store_attribution_install_referrer_response
    ) AS `text2`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.text.meta_attribution_data,
      metrics.text.play_store_attribution_install_referrer_response
    ) AS `text`
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
            STRUCT(experiments.value.extra.type, experiments.value.extra.enrollment_id) AS `extra`
          ) AS `value`
        )
      FROM
        UNNEST(ping_info.experiments) AS `experiments`
    ) AS `experiments`,
    ping_info.ping_type,
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    ping_info.server_knobs_config,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.first_session`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  STRUCT(
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.telemetry_sdk_build,
    client_info.build_date,
    client_info.windows_build_number,
    client_info.session_count,
    client_info.session_id,
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
  document_id,
  events,
  STRUCT(
    metadata.geo,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      metadata.header.parsed_date,
      metadata.header.parsed_x_source_tags,
      metadata.header.parsed_x_lb_tags
    ) AS `header`,
    metadata.isp,
    metadata.user_agent
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.datetime.first_session_timestamp,
      metrics.datetime.raw_first_session_timestamp
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.first_session_adgroup,
      metrics.string.first_session_campaign,
      metrics.string.first_session_creative,
      metrics.string.first_session_network,
      metrics.string.first_session_distribution_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.meta_attribution_app,
      metrics.string.meta_attribution_nonce,
      metrics.string.meta_attribution_t,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(
      STRUCT(
        CAST(NULL AS INTEGER) AS `bucket_count`,
        CAST(NULL AS INTEGER) AS `count`,
        CAST(NULL AS STRING) AS `histogram_type`,
        CAST(NULL AS INTEGER) AS `overflow`,
        CAST(NULL AS ARRAY<FLOAT64>) AS `range`,
        metrics.timing_distribution.first_session_adjust_attribution_time.sum,
        CAST(NULL AS STRING) AS `time_unit`,
        CAST(NULL AS INTEGER) AS `underflow`,
        metrics.timing_distribution.first_session_adjust_attribution_time.values
      ) AS `first_session_adjust_attribution_time`
    ) AS `timing_distribution`,
    STRUCT(metrics.timespan.first_session_adjust_attribution_timespan) AS `timespan`,
    STRUCT(
      metrics.text2.meta_attribution_data,
      metrics.text2.play_store_attribution_install_referrer_response
    ) AS `text2`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.text.meta_attribution_data,
      metrics.text.play_store_attribution_install_referrer_response
    ) AS `text`
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
            STRUCT(experiments.value.extra.type, experiments.value.extra.enrollment_id) AS `extra`
          ) AS `value`
        )
      FROM
        UNNEST(ping_info.experiments) AS `experiments`
    ) AS `experiments`,
    ping_info.ping_type,
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    ping_info.server_knobs_config,
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.first_session`
