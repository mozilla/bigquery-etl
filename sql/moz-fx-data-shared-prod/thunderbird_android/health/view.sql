-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.thunderbird_android.health`
AS
SELECT
  "net_thunderbird_android" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_health_init_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.glean_health_file_read_error,
      metrics.labeled_counter.glean_health_file_write_error
    ) AS `labeled_counter`,
    STRUCT(
      metrics.memory_distribution.glean_database_size,
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.object.glean_health_data_directory_info,
      metrics.object.glean_database_load_sizes
    ) AS `object`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error,
      metrics.string.glean_health_exception_state
    ) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.timing_distribution.glean_database_write_time,
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_validation_shutdown_wait
    ) AS `timing_distribution`,
    STRUCT(metrics.uuid.glean_health_recovered_client_id) AS `uuid`
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
  `moz-fx-data-shared-prod.net_thunderbird_android.health`
UNION ALL
SELECT
  "net_thunderbird_android_beta" AS normalized_app_id,
  "beta" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_health_init_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.glean_health_file_read_error,
      metrics.labeled_counter.glean_health_file_write_error
    ) AS `labeled_counter`,
    STRUCT(
      metrics.memory_distribution.glean_database_size,
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.object.glean_health_data_directory_info,
      metrics.object.glean_database_load_sizes
    ) AS `object`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error,
      metrics.string.glean_health_exception_state
    ) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.timing_distribution.glean_database_write_time,
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_validation_shutdown_wait
    ) AS `timing_distribution`,
    STRUCT(metrics.uuid.glean_health_recovered_client_id) AS `uuid`
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
  `moz-fx-data-shared-prod.net_thunderbird_android_beta.health`
UNION ALL
SELECT
  "net_thunderbird_android_daily" AS normalized_app_id,
  "nightly" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_health_init_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.glean_health_file_read_error,
      metrics.labeled_counter.glean_health_file_write_error
    ) AS `labeled_counter`,
    STRUCT(
      metrics.memory_distribution.glean_database_size,
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.object.glean_health_data_directory_info,
      metrics.object.glean_database_load_sizes
    ) AS `object`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error,
      metrics.string.glean_health_exception_state
    ) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`,
    STRUCT(
      metrics.timing_distribution.glean_database_write_time,
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_validation_shutdown_wait
    ) AS `timing_distribution`,
    STRUCT(metrics.uuid.glean_health_recovered_client_id) AS `uuid`
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
  `moz-fx-data-shared-prod.net_thunderbird_android_daily.health`
