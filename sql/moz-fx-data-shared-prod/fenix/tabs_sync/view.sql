-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.tabs_sync`
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
      metrics.counter.tabs_sync_outgoing_batches,
      metrics.counter.tabs_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.tabs_sync_finished_at,
      metrics.datetime.raw_tabs_sync_finished_at,
      metrics.datetime.tabs_sync_started_at,
      metrics.datetime.raw_tabs_sync_started_at,
      metrics.datetime.tabs_sync_v2_finished_at,
      metrics.datetime.raw_tabs_sync_v2_finished_at,
      metrics.datetime.tabs_sync_v2_started_at,
      metrics.datetime.raw_tabs_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.tabs_sync_incoming,
      metrics.labeled_counter.tabs_sync_outgoing,
      metrics.labeled_counter.tabs_sync_v2_incoming,
      metrics.labeled_counter.tabs_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.tabs_sync_failure_reason,
      metrics.labeled_string.tabs_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.tabs_sync_uid,
      metrics.string.tabs_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.tabs_sync`
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
      metrics.counter.tabs_sync_outgoing_batches,
      metrics.counter.tabs_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.tabs_sync_finished_at,
      metrics.datetime.raw_tabs_sync_finished_at,
      metrics.datetime.tabs_sync_started_at,
      metrics.datetime.raw_tabs_sync_started_at,
      metrics.datetime.tabs_sync_v2_finished_at,
      metrics.datetime.raw_tabs_sync_v2_finished_at,
      metrics.datetime.tabs_sync_v2_started_at,
      metrics.datetime.raw_tabs_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.tabs_sync_incoming,
      metrics.labeled_counter.tabs_sync_outgoing,
      metrics.labeled_counter.tabs_sync_v2_incoming,
      metrics.labeled_counter.tabs_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.tabs_sync_failure_reason,
      metrics.labeled_string.tabs_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.tabs_sync_uid,
      metrics.string.tabs_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.tabs_sync`
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
      metrics.counter.tabs_sync_outgoing_batches,
      metrics.counter.tabs_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.tabs_sync_finished_at,
      metrics.datetime.raw_tabs_sync_finished_at,
      metrics.datetime.tabs_sync_started_at,
      metrics.datetime.raw_tabs_sync_started_at,
      metrics.datetime.tabs_sync_v2_finished_at,
      metrics.datetime.raw_tabs_sync_v2_finished_at,
      metrics.datetime.tabs_sync_v2_started_at,
      metrics.datetime.raw_tabs_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.tabs_sync_incoming,
      metrics.labeled_counter.tabs_sync_outgoing,
      metrics.labeled_counter.tabs_sync_v2_incoming,
      metrics.labeled_counter.tabs_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.tabs_sync_failure_reason,
      metrics.labeled_string.tabs_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.tabs_sync_uid,
      metrics.string.tabs_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.tabs_sync`
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
      metrics.counter.tabs_sync_outgoing_batches,
      metrics.counter.tabs_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.tabs_sync_finished_at,
      metrics.datetime.raw_tabs_sync_finished_at,
      metrics.datetime.tabs_sync_started_at,
      metrics.datetime.raw_tabs_sync_started_at,
      metrics.datetime.tabs_sync_v2_finished_at,
      metrics.datetime.raw_tabs_sync_v2_finished_at,
      metrics.datetime.tabs_sync_v2_started_at,
      metrics.datetime.raw_tabs_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.tabs_sync_incoming,
      metrics.labeled_counter.tabs_sync_outgoing,
      metrics.labeled_counter.tabs_sync_v2_incoming,
      metrics.labeled_counter.tabs_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.tabs_sync_failure_reason,
      metrics.labeled_string.tabs_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.tabs_sync_uid,
      metrics.string.tabs_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.tabs_sync`
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
      metrics.counter.tabs_sync_outgoing_batches,
      metrics.counter.tabs_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.tabs_sync_finished_at,
      metrics.datetime.raw_tabs_sync_finished_at,
      metrics.datetime.tabs_sync_started_at,
      metrics.datetime.raw_tabs_sync_started_at,
      metrics.datetime.tabs_sync_v2_finished_at,
      metrics.datetime.raw_tabs_sync_v2_finished_at,
      metrics.datetime.tabs_sync_v2_started_at,
      metrics.datetime.raw_tabs_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.tabs_sync_incoming,
      metrics.labeled_counter.tabs_sync_outgoing,
      metrics.labeled_counter.tabs_sync_v2_incoming,
      metrics.labeled_counter.tabs_sync_v2_outgoing
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.tabs_sync_failure_reason,
      metrics.labeled_string.tabs_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.tabs_sync_uid,
      metrics.string.tabs_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.tabs_sync`