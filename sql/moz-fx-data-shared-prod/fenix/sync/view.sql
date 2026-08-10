-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.sync`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.sync_failure_reason,
      metrics.labeled_string.sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.syncs_hashed_device_id,
      metrics.string.syncs_hashed_fxa_uid,
      metrics.string.syncs_sync_node_type,
      metrics.string.system_os_distro,
      metrics.string.system_os_distro_version,
      metrics.string.system_os_locale,
      metrics.string.system_os_name,
      metrics.string.system_os_version,
      metrics.string.system_os_libstdcxx_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(
      metrics.datetime.syncs_session_start_date,
      metrics.datetime.raw_syncs_session_start_date
    ) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.sync`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.sync_failure_reason,
      metrics.labeled_string.sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.syncs_hashed_device_id,
      metrics.string.syncs_hashed_fxa_uid,
      metrics.string.syncs_sync_node_type,
      metrics.string.system_os_distro,
      metrics.string.system_os_distro_version,
      metrics.string.system_os_locale,
      metrics.string.system_os_name,
      metrics.string.system_os_version,
      metrics.string.system_os_libstdcxx_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(
      metrics.datetime.syncs_session_start_date,
      metrics.datetime.raw_syncs_session_start_date
    ) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.sync`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.sync_failure_reason,
      metrics.labeled_string.sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.syncs_hashed_device_id,
      metrics.string.syncs_hashed_fxa_uid,
      metrics.string.syncs_sync_node_type,
      metrics.string.system_os_distro,
      metrics.string.system_os_distro_version,
      metrics.string.system_os_locale,
      metrics.string.system_os_name,
      metrics.string.system_os_version,
      metrics.string.system_os_libstdcxx_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(
      metrics.datetime.syncs_session_start_date,
      metrics.datetime.raw_syncs_session_start_date
    ) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.sync`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
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
    client_info.session_id,
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
  document_id,
  events,
  STRUCT(
    metadata.geo,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      metadata.header.parsed_date,
      metadata.header.parsed_x_source_tags,
      metadata.header.parsed_x_lb_tags
    ) AS `header`,
    metadata.user_agent,
    metadata.isp
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.sync_failure_reason,
      metrics.labeled_string.sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.syncs_hashed_device_id,
      metrics.string.syncs_hashed_fxa_uid,
      metrics.string.syncs_sync_node_type,
      metrics.string.system_os_distro,
      metrics.string.system_os_distro_version,
      metrics.string.system_os_locale,
      metrics.string.system_os_name,
      metrics.string.system_os_version,
      metrics.string.system_os_libstdcxx_version
    ) AS `string`,
    STRUCT(
      STRUCT(
        CAST(NULL AS INTEGER) AS `count`,
        metrics.custom_distribution.pwmgr_login_page_safety.sum,
        metrics.custom_distribution.pwmgr_login_page_safety.values
      ) AS `pwmgr_login_page_safety`,
      STRUCT(
        CAST(NULL AS INTEGER) AS `count`,
        metrics.custom_distribution.pwmgr_prompt_remember_action.sum,
        metrics.custom_distribution.pwmgr_prompt_remember_action.values
      ) AS `pwmgr_prompt_remember_action`,
      STRUCT(
        CAST(NULL AS INTEGER) AS `count`,
        metrics.custom_distribution.pwmgr_prompt_update_action.sum,
        metrics.custom_distribution.pwmgr_prompt_update_action.values
      ) AS `pwmgr_prompt_update_action`
    ) AS `custom_distribution`,
    STRUCT(
      metrics.datetime.syncs_session_start_date,
      metrics.datetime.raw_syncs_session_start_date
    ) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
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
    ping_info.server_knobs_config,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.sync`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
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
    client_info.session_id,
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
  document_id,
  events,
  STRUCT(
    metadata.geo,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      metadata.header.parsed_date,
      metadata.header.parsed_x_source_tags,
      metadata.header.parsed_x_lb_tags
    ) AS `header`,
    metadata.user_agent,
    metadata.isp
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.sync_failure_reason,
      metrics.labeled_string.sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
    STRUCT(
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.syncs_hashed_device_id,
      metrics.string.syncs_hashed_fxa_uid,
      metrics.string.syncs_sync_node_type,
      metrics.string.system_os_distro,
      metrics.string.system_os_distro_version,
      metrics.string.system_os_locale,
      metrics.string.system_os_name,
      metrics.string.system_os_version,
      metrics.string.system_os_libstdcxx_version
    ) AS `string`,
    STRUCT(
      STRUCT(
        CAST(NULL AS INTEGER) AS `count`,
        metrics.custom_distribution.pwmgr_login_page_safety.sum,
        metrics.custom_distribution.pwmgr_login_page_safety.values
      ) AS `pwmgr_login_page_safety`,
      STRUCT(
        CAST(NULL AS INTEGER) AS `count`,
        metrics.custom_distribution.pwmgr_prompt_remember_action.sum,
        metrics.custom_distribution.pwmgr_prompt_remember_action.values
      ) AS `pwmgr_prompt_remember_action`,
      STRUCT(
        CAST(NULL AS INTEGER) AS `count`,
        metrics.custom_distribution.pwmgr_prompt_update_action.sum,
        metrics.custom_distribution.pwmgr_prompt_update_action.values
      ) AS `pwmgr_prompt_update_action`
    ) AS `custom_distribution`,
    STRUCT(
      metrics.datetime.syncs_session_start_date,
      metrics.datetime.raw_syncs_session_start_date
    ) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
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
    ping_info.server_knobs_config,
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.sync`
