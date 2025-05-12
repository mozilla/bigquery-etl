-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.metrics`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.glean_error_preinit_tasks_timeout,
      metrics.boolean.settings_connect_on_startup_active,
      metrics.boolean.settings_using_system_language,
      CAST(NULL AS BOOLEAN) AS `glean_core_migration_successful`
    ) AS `boolean`,
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_time_invalid_timezone_offset,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_validation_foreground_count,
      CAST(NULL AS INTEGER) AS `glean_validation_app_forceclosed_count`,
      CAST(NULL AS INTEGER) AS `glean_validation_baseline_ping_count`
    ) AS `counter`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.memory_distribution.glean_database_size,
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.string.ping_reason,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_validation_shutdown_wait,
      metrics.timing_distribution.performance_time_to_main_screen,
      metrics.timing_distribution.glean_database_write_time
    ) AS `timing_distribution`
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
  `moz-fx-data-shared-prod.mozillavpn.metrics`
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.glean_error_preinit_tasks_timeout,
      metrics.boolean.settings_connect_on_startup_active,
      metrics.boolean.settings_using_system_language,
      metrics.boolean.glean_core_migration_successful
    ) AS `boolean`,
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_time_invalid_timezone_offset,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_validation_foreground_count,
      metrics.counter.glean_validation_app_forceclosed_count,
      metrics.counter.glean_validation_baseline_ping_count
    ) AS `counter`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      STRUCT(
        metrics.memory_distribution.glean_database_size.count,
        metrics.memory_distribution.glean_database_size.sum,
        metrics.memory_distribution.glean_database_size.values
      ) AS `glean_database_size`,
      STRUCT(
        metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size.count,
        metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size.sum,
        metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size.values
      ) AS `glean_upload_discarded_exceeding_pings_size`,
      STRUCT(
        metrics.memory_distribution.glean_upload_pending_pings_directory_size.count,
        metrics.memory_distribution.glean_upload_pending_pings_directory_size.sum,
        metrics.memory_distribution.glean_upload_pending_pings_directory_size.values
      ) AS `glean_upload_pending_pings_directory_size`
    ) AS `memory_distribution`,
    STRUCT(
      metrics.string.ping_reason,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_validation_shutdown_wait,
      metrics.timing_distribution.performance_time_to_main_screen,
      metrics.timing_distribution.glean_database_write_time
    ) AS `timing_distribution`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.metrics`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.glean_error_preinit_tasks_timeout,
      metrics.boolean.settings_connect_on_startup_active,
      metrics.boolean.settings_using_system_language,
      CAST(NULL AS BOOLEAN) AS `glean_core_migration_successful`
    ) AS `boolean`,
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_time_invalid_timezone_offset,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_validation_foreground_count,
      CAST(NULL AS INTEGER) AS `glean_validation_app_forceclosed_count`,
      CAST(NULL AS INTEGER) AS `glean_validation_baseline_ping_count`
    ) AS `counter`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.memory_distribution.glean_database_size,
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.string.ping_reason,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_validation_shutdown_wait,
      metrics.timing_distribution.performance_time_to_main_screen,
      metrics.timing_distribution.glean_database_write_time
    ) AS `timing_distribution`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn.metrics`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn_network_extension" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.glean_error_preinit_tasks_timeout,
      CAST(NULL AS BOOLEAN) AS `settings_connect_on_startup_active`,
      CAST(NULL AS BOOLEAN) AS `settings_using_system_language`,
      CAST(NULL AS BOOLEAN) AS `glean_core_migration_successful`
    ) AS `boolean`,
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_time_invalid_timezone_offset,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_validation_foreground_count,
      CAST(NULL AS INTEGER) AS `glean_validation_app_forceclosed_count`,
      CAST(NULL AS INTEGER) AS `glean_validation_baseline_ping_count`
    ) AS `counter`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.memory_distribution.glean_database_size,
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.string.ping_reason,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_validation_shutdown_wait,
      metrics.timing_distribution.performance_time_to_main_screen,
      metrics.timing_distribution.glean_database_write_time
    ) AS `timing_distribution`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.metrics`
