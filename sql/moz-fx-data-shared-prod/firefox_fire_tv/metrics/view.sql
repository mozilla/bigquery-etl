-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_fire_tv.metrics`
AS
SELECT
  "org_mozilla_tv_firefox" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.boolean.glean_core_migration_successful,
      metrics.boolean.glean_error_preinit_tasks_timeout
    ) AS `boolean`,
    STRUCT(
      metrics.counter.glean_validation_baseline_ping_count,
      metrics.counter.glean_validation_app_forceclosed_count,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_validation_foreground_count,
      metrics.counter.glean_error_io,
      metrics.counter.glean_time_invalid_timezone_offset,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids
    ) AS `counter`,
    STRUCT(
      metrics.string.ping_reason,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error
    ) AS `string`,
    STRUCT(
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size,
      metrics.memory_distribution.glean_database_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_wait,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait
    ) AS `timing_distribution`
  ) AS `metrics`
FROM
  `moz-fx-data-shared-prod.org_mozilla_tv_firefox.metrics`