-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.addresses_sync`
AS
SELECT
  "org_mozilla_ios_firefox" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.addresses_sync_outgoing_batches,
      metrics.counter.addresses_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.addresses_sync_finished_at,
      metrics.datetime.raw_addresses_sync_finished_at,
      metrics.datetime.addresses_sync_started_at,
      metrics.datetime.raw_addresses_sync_started_at,
      metrics.datetime.addresses_sync_v2_finished_at,
      metrics.datetime.raw_addresses_sync_v2_finished_at,
      metrics.datetime.addresses_sync_v2_started_at,
      metrics.datetime.raw_addresses_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.addresses_sync_incoming,
      metrics.labeled_counter.addresses_sync_outgoing,
      metrics.labeled_counter.addresses_sync_v2_incoming,
      metrics.labeled_counter.addresses_sync_v2_outgoing,
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.addresses_sync_failure_reason,
      metrics.labeled_string.addresses_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.addresses_sync_uid,
      metrics.string.addresses_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.addresses_sync`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  "beta" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.addresses_sync_outgoing_batches,
      metrics.counter.addresses_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.addresses_sync_finished_at,
      metrics.datetime.raw_addresses_sync_finished_at,
      metrics.datetime.addresses_sync_started_at,
      metrics.datetime.raw_addresses_sync_started_at,
      metrics.datetime.addresses_sync_v2_finished_at,
      metrics.datetime.raw_addresses_sync_v2_finished_at,
      metrics.datetime.addresses_sync_v2_started_at,
      metrics.datetime.raw_addresses_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.addresses_sync_incoming,
      metrics.labeled_counter.addresses_sync_outgoing,
      metrics.labeled_counter.addresses_sync_v2_incoming,
      metrics.labeled_counter.addresses_sync_v2_outgoing,
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.addresses_sync_failure_reason,
      metrics.labeled_string.addresses_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.addresses_sync_uid,
      metrics.string.addresses_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.addresses_sync`
UNION ALL
SELECT
  "org_mozilla_ios_fennec" AS normalized_app_id,
  "nightly" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.addresses_sync_outgoing_batches,
      metrics.counter.addresses_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.addresses_sync_finished_at,
      metrics.datetime.raw_addresses_sync_finished_at,
      metrics.datetime.addresses_sync_started_at,
      metrics.datetime.raw_addresses_sync_started_at,
      metrics.datetime.addresses_sync_v2_finished_at,
      metrics.datetime.raw_addresses_sync_v2_finished_at,
      metrics.datetime.addresses_sync_v2_started_at,
      metrics.datetime.raw_addresses_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.addresses_sync_incoming,
      metrics.labeled_counter.addresses_sync_outgoing,
      metrics.labeled_counter.addresses_sync_v2_incoming,
      metrics.labeled_counter.addresses_sync_v2_outgoing,
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.addresses_sync_failure_reason,
      metrics.labeled_string.addresses_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.addresses_sync_uid,
      metrics.string.addresses_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.addresses_sync`
