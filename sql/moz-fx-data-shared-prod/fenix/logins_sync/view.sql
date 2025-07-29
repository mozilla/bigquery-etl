-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.logins_sync`
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
      metrics.counter.logins_sync_outgoing_batches,
      metrics.counter.logins_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.logins_sync_finished_at,
      metrics.datetime.raw_logins_sync_finished_at,
      metrics.datetime.logins_sync_started_at,
      metrics.datetime.raw_logins_sync_started_at,
      metrics.datetime.logins_sync_v2_finished_at,
      metrics.datetime.raw_logins_sync_v2_finished_at,
      metrics.datetime.logins_sync_v2_started_at,
      metrics.datetime.raw_logins_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.logins_sync_incoming,
      metrics.labeled_counter.logins_sync_outgoing,
      metrics.labeled_counter.logins_sync_v2_incoming,
      metrics.labeled_counter.logins_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.logins_sync_failure_reason,
      metrics.labeled_string.logins_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.logins_sync_uid,
      metrics.string.logins_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.logins_sync`
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
      metrics.counter.logins_sync_outgoing_batches,
      metrics.counter.logins_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.logins_sync_finished_at,
      metrics.datetime.raw_logins_sync_finished_at,
      metrics.datetime.logins_sync_started_at,
      metrics.datetime.raw_logins_sync_started_at,
      metrics.datetime.logins_sync_v2_finished_at,
      metrics.datetime.raw_logins_sync_v2_finished_at,
      metrics.datetime.logins_sync_v2_started_at,
      metrics.datetime.raw_logins_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.logins_sync_incoming,
      metrics.labeled_counter.logins_sync_outgoing,
      metrics.labeled_counter.logins_sync_v2_incoming,
      metrics.labeled_counter.logins_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.logins_sync_failure_reason,
      metrics.labeled_string.logins_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.logins_sync_uid,
      metrics.string.logins_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.logins_sync`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
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
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.logins_sync_outgoing_batches,
      metrics.counter.logins_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.logins_sync_finished_at,
      metrics.datetime.raw_logins_sync_finished_at,
      metrics.datetime.logins_sync_started_at,
      metrics.datetime.raw_logins_sync_started_at,
      metrics.datetime.logins_sync_v2_finished_at,
      metrics.datetime.raw_logins_sync_v2_finished_at,
      metrics.datetime.logins_sync_v2_started_at,
      metrics.datetime.raw_logins_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.logins_sync_incoming,
      metrics.labeled_counter.logins_sync_outgoing,
      metrics.labeled_counter.logins_sync_v2_incoming,
      metrics.labeled_counter.logins_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.logins_sync_failure_reason,
      metrics.labeled_string.logins_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.logins_sync_uid,
      metrics.string.logins_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
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
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.logins_sync`
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
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.logins_sync_outgoing_batches,
      metrics.counter.logins_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.logins_sync_finished_at,
      metrics.datetime.raw_logins_sync_finished_at,
      metrics.datetime.logins_sync_started_at,
      metrics.datetime.raw_logins_sync_started_at,
      metrics.datetime.logins_sync_v2_finished_at,
      metrics.datetime.raw_logins_sync_v2_finished_at,
      metrics.datetime.logins_sync_v2_started_at,
      metrics.datetime.raw_logins_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.logins_sync_incoming,
      metrics.labeled_counter.logins_sync_outgoing,
      metrics.labeled_counter.logins_sync_v2_incoming,
      metrics.labeled_counter.logins_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.logins_sync_failure_reason,
      metrics.labeled_string.logins_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.logins_sync_uid,
      metrics.string.logins_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
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
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.logins_sync`
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
      metrics.counter.logins_sync_outgoing_batches,
      metrics.counter.logins_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.logins_sync_finished_at,
      metrics.datetime.raw_logins_sync_finished_at,
      metrics.datetime.logins_sync_started_at,
      metrics.datetime.raw_logins_sync_started_at,
      metrics.datetime.logins_sync_v2_finished_at,
      metrics.datetime.raw_logins_sync_v2_finished_at,
      metrics.datetime.logins_sync_v2_started_at,
      metrics.datetime.raw_logins_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.logins_sync_incoming,
      metrics.labeled_counter.logins_sync_outgoing,
      metrics.labeled_counter.logins_sync_v2_incoming,
      metrics.labeled_counter.logins_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.logins_sync_failure_reason,
      metrics.labeled_string.logins_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.logins_sync_uid,
      metrics.string.logins_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`,
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.logins_sync`
