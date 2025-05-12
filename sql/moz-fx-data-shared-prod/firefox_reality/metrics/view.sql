-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality.metrics`
AS
SELECT
  "org_mozilla_vrbrowser" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.glean_core_migration_successful,
      metrics.boolean.glean_error_preinit_tasks_timeout,
      metrics.boolean.firefox_account_bookmarks_sync_status,
      metrics.boolean.firefox_account_history_sync_status
    ) AS `boolean`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.firefox_account_received_tab,
      metrics.labeled_counter.searches_counts,
      metrics.labeled_counter.url_query_type,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.distribution_channel_name,
      metrics.string.ping_reason,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error
    ) AS `string`,
    STRUCT(
      metrics.counter.glean_validation_baseline_ping_count,
      metrics.counter.firefox_account_tab_sent,
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
      metrics.timing_distribution.immersive_duration,
      metrics.timing_distribution.pages_page_load,
      metrics.timing_distribution.windows_duration,
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_wait,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_database_write_time
    ) AS `timing_distribution`,
    STRUCT(
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size,
      metrics.memory_distribution.glean_database_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`
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
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser.metrics`
