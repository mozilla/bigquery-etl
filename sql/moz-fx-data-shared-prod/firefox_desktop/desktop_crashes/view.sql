CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.desktop_crashes`
AS
SELECT
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  metrics,
  normalized_app_name,
  normalized_channel,
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
  `moz-fx-data-shared-prod.firefox_desktop.crash`
UNION ALL
SELECT
  additional_properties,
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
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.labeled_counter,
    STRUCT(
      metrics.boolean.crash_startup,
      metrics.boolean.crash_is_garbage_collecting,
      metrics.boolean.crash_windows_error_reporting,
      metrics.boolean.dll_blocklist_init_failed,
      metrics.boolean.dll_blocklist_user32_loaded_before,
      metrics.boolean.environment_headless_mode
    ) AS `boolean`,
    metrics.datetime,
    STRUCT(
      metrics.string.crash_process_type,
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
      metrics.string.crash_remote_type,
      metrics.string.crash_shutdown_progress,
      metrics.string.crash_windows_file_dialog_error_code,
      metrics.string.windows_package_family_name,
      metrics.string.memory_js_large_allocation_failure,
      metrics.string.memory_js_out_of_memory
    ) AS `string`,
    STRUCT(
      CAST(NULL AS STRUCT<`time_unit` STRING, `value` INTEGER>) AS `crash_uptime`,
      metrics.timespan.environment_uptime
    ) AS `timespan`,
    metrics.object,
    metrics.quantity,
    metrics.string_list
  ) AS `metrics`,
  normalized_app_name,
  normalized_channel,
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
  `moz-fx-data-shared-prod.firefox_crashreporter.crash`
