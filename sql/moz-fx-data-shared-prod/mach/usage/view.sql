-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mach.usage`
AS
SELECT
  "mozilla_mach" AS normalized_app_id,
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
      metrics.boolean.mach_success,
      metrics.boolean.mozbuild_artifact,
      metrics.boolean.mozbuild_ccache,
      metrics.boolean.mozbuild_clobber,
      metrics.boolean.mozbuild_debug,
      metrics.boolean.mozbuild_icecream,
      metrics.boolean.mozbuild_opt,
      metrics.boolean.mozbuild_sccache,
      metrics.boolean.mach_system_ssh_connection,
      metrics.boolean.mach_system_vscode_terminal,
      metrics.boolean.mach_system_vscode_running,
      metrics.boolean.mach_moz_automation
    ) AS `boolean`,
    STRUCT(
      metrics.counter.mach_system_logical_cores,
      metrics.counter.mach_system_physical_cores
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.memory_distribution.mach_system_memory) AS `memory_distribution`,
    STRUCT(
      metrics.string.mach_command,
      metrics.string.mach_system_cpu_brand,
      metrics.string.mozbuild_compiler,
      metrics.string.mach_system_distro,
      metrics.string.mach_system_distro_version,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.mozbuild_project,
      metrics.string.mozbuild_target
    ) AS `string`,
    STRUCT(metrics.string_list.mach_argv) AS `string_list`,
    STRUCT(
      metrics.timespan.mach_duration,
      metrics.timespan.mozbuild_tier_artifact_duration,
      metrics.timespan.mozbuild_tier_compile_duration,
      metrics.timespan.mozbuild_tier_configure_duration,
      metrics.timespan.mozbuild_tier_export_duration,
      metrics.timespan.mozbuild_tier_libs_duration,
      metrics.timespan.mozbuild_tier_misc_duration,
      metrics.timespan.mozbuild_tier_pre_export_duration,
      metrics.timespan.mozbuild_tier_tools_duration
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
  `moz-fx-data-shared-prod.mozilla_mach.usage`
