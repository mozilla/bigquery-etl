-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.bergamot.custom`
AS
SELECT
  "org_mozilla_bergamot" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
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
  `moz-fx-data-shared-prod.org_mozilla_bergamot.custom`
