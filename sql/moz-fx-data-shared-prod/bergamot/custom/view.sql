-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.bergamot.custom`
AS
SELECT
  "org_mozilla_bergamot" AS normalized_app_id,
  normalized_channel,
  CAST(NULL AS STRING) AS `additional_properties`,
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
    client_info.session_id
  ) AS `client_info`,
  CAST(NULL AS STRING) AS `document_id`,
  events,
  STRUCT(metadata.geo, metadata.header, metadata.isp, metadata.user_agent) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.counter.errors_marian,
      metrics.counter.errors_memory,
      metrics.counter.errors_model_download,
      metrics.counter.errors_translation,
      metrics.counter.service_lang_mismatch,
      metrics.counter.service_not_suppported,
      metrics.counter.test_counter_test,
      metrics.counter.service_not_supported
    ) AS `counter`,
    STRUCT(
      metrics.string.metadata_from_lang,
      metrics.string.metadata_to_lang,
      metrics.string.performance_model_download_time,
      metrics.string.performance_model_load_time,
      metrics.string.performance_translation_quality,
      metrics.string.performance_translation_time,
      metrics.string.performance_words_per_second,
      metrics.string.test_string_test,
      metrics.string.metadata_firefox_client_id,
      metrics.string.metadata_bergamot_translator_version,
      metrics.string.metadata_cpu_extensions,
      metrics.string.metadata_cpu_family,
      metrics.string.metadata_cpu_model,
      metrics.string.metadata_cpu_stepping,
      metrics.string.metadata_cpu_vendor,
      metrics.string.metadata_extension_build_id,
      metrics.string.metadata_extension_version,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.metadata_cpu_cores_count,
      metrics.quantity.metadata_cpu_count,
      metrics.quantity.metadata_cpu_l2_cache,
      metrics.quantity.metadata_cpu_l3_cache,
      metrics.quantity.metadata_cpu_speed,
      metrics.quantity.metadata_system_memory,
      metrics.quantity.performance_full_page_translated_wps,
      metrics.quantity.performance_translation_engine_wps,
      metrics.quantity.performance_word_count,
      metrics.quantity.performance_word_count_visible_in_viewport,
      metrics.quantity.metadata_cpu_family,
      metrics.quantity.metadata_cpu_model,
      metrics.quantity.metadata_cpu_stepping
    ) AS `quantity`,
    STRUCT(
      metrics.timespan.performance_full_page_translated_time,
      metrics.timespan.performance_model_download_time_num,
      metrics.timespan.performance_model_load_time_num,
      metrics.timespan.performance_translation_engine_time
    ) AS `timespan`
  ) AS `metrics`,
  CAST(NULL AS STRING) AS `normalized_app_name`,
  CAST(NULL AS STRING) AS `normalized_channel`,
  CAST(NULL AS STRING) AS `normalized_country_code`,
  CAST(NULL AS STRING) AS `normalized_os`,
  CAST(NULL AS STRING) AS `normalized_os_version`,
  STRUCT(
    ping_info.end_time,
    ping_info.experiments,
    ping_info.ping_type,
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    ping_info.parsed_start_time,
    ping_info.parsed_end_time
  ) AS `ping_info`,
  CAST(NULL AS INTEGER) AS `sample_id`,
  CAST(NULL AS TIMESTAMP) AS `submission_timestamp`
FROM
  `moz-fx-data-shared-prod.org_mozilla_bergamot.custom`
