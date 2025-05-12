-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mach.usage`
AS
SELECT
  "mozilla_mach" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
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
  `moz-fx-data-shared-prod.mozilla_mach.usage`
