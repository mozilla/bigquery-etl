-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.crash`
AS
SELECT
  "org_mozilla_klar" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.crash_startup,
      metrics.boolean.crash_is_garbage_collecting,
      metrics.boolean.environment_headless_mode
    ) AS `boolean`,
    STRUCT(metrics.datetime.crash_time, metrics.datetime.raw_crash_time) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.crash_process_type,
      metrics.string.crash_cause,
      metrics.string.crash_remote_type,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.crash_app_build,
      metrics.string.crash_app_channel,
      metrics.string.crash_app_display_version,
      metrics.string.crash_background_task_name,
      metrics.string.crash_font_name,
      metrics.string.crash_ipc_channel_error,
      metrics.string.crash_main_thread_runnable_name,
      metrics.string.crash_minidump_sha256_hash,
      metrics.string.crash_moz_crash_reason,
      metrics.string.crash_profiler_child_shutdown_phase,
      metrics.string.crash_shutdown_progress,
      metrics.string.memory_js_large_allocation_failure,
      metrics.string.memory_js_out_of_memory
    ) AS `string`,
    STRUCT(metrics.timespan.crash_uptime, metrics.timespan.environment_uptime) AS `timespan`,
    STRUCT(
      metrics.object.crash_async_shutdown_timeout,
      metrics.object.crash_breadcrumbs,
      metrics.object.crash_java_exception,
      metrics.object.crash_quota_manager_shutdown_timeout,
      metrics.object.crash_stack_traces
    ) AS `object`,
    STRUCT(
      metrics.quantity.crash_event_loop_nesting_level,
      metrics.quantity.crash_gpu_process_launch,
      metrics.quantity.memory_available_commit,
      metrics.quantity.memory_available_physical,
      metrics.quantity.memory_available_swap,
      metrics.quantity.memory_available_virtual,
      metrics.quantity.memory_low_physical,
      metrics.quantity.memory_oom_allocation_size,
      metrics.quantity.memory_purgeable_physical,
      metrics.quantity.memory_system_use_percentage,
      metrics.quantity.memory_texture,
      metrics.quantity.memory_total_page_file,
      metrics.quantity.memory_total_physical,
      metrics.quantity.memory_total_virtual
    ) AS `quantity`,
    STRUCT(metrics.string_list.environment_experimental_features) AS `string_list`
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
  `moz-fx-data-shared-prod.org_mozilla_klar.crash`
