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
      metrics.string.system_os_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(metrics.datetime.syncs_session_start_date) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`
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
      metrics.string.system_os_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(metrics.datetime.syncs_session_start_date) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`
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
      metrics.string.system_os_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(metrics.datetime.syncs_session_start_date) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`
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
      metrics.string.system_os_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(metrics.datetime.syncs_session_start_date) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.sync`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
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
      metrics.string.system_os_version
    ) AS `string`,
    STRUCT(
      metrics.custom_distribution.pwmgr_login_page_safety,
      metrics.custom_distribution.pwmgr_prompt_remember_action,
      metrics.custom_distribution.pwmgr_prompt_update_action
    ) AS `custom_distribution`,
    STRUCT(metrics.datetime.syncs_session_start_date) AS `datetime`,
    STRUCT(metrics.object.syncs_migrations, metrics.object.syncs_syncs) AS `object`,
    STRUCT(
      metrics.quantity.syncs_discarded,
      metrics.quantity.system_os_service_pack_major,
      metrics.quantity.system_os_service_pack_minor,
      metrics.quantity.system_os_windows_build_number,
      metrics.quantity.system_os_windows_ubr
    ) AS `quantity`
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.sync`
