-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.bookmarks_sync`
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
      metrics.counter.bookmarks_sync_outgoing_batches,
      metrics.counter.bookmarks_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.bookmarks_sync_finished_at,
      metrics.datetime.raw_bookmarks_sync_finished_at,
      metrics.datetime.bookmarks_sync_started_at,
      metrics.datetime.raw_bookmarks_sync_started_at,
      metrics.datetime.bookmarks_sync_v2_finished_at,
      metrics.datetime.raw_bookmarks_sync_v2_finished_at,
      metrics.datetime.bookmarks_sync_v2_started_at,
      metrics.datetime.raw_bookmarks_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.bookmarks_sync_incoming,
      metrics.labeled_counter.bookmarks_sync_outgoing,
      metrics.labeled_counter.bookmarks_sync_remote_tree_problems,
      metrics.labeled_counter.bookmarks_sync_v2_incoming,
      metrics.labeled_counter.bookmarks_sync_v2_outgoing,
      metrics.labeled_counter.bookmarks_sync_v2_remote_tree_problems,
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.bookmarks_sync_failure_reason,
      metrics.labeled_string.bookmarks_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.bookmarks_sync_uid,
      metrics.string.bookmarks_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.bookmarks_sync`
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
      metrics.counter.bookmarks_sync_outgoing_batches,
      metrics.counter.bookmarks_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.bookmarks_sync_finished_at,
      metrics.datetime.raw_bookmarks_sync_finished_at,
      metrics.datetime.bookmarks_sync_started_at,
      metrics.datetime.raw_bookmarks_sync_started_at,
      metrics.datetime.bookmarks_sync_v2_finished_at,
      metrics.datetime.raw_bookmarks_sync_v2_finished_at,
      metrics.datetime.bookmarks_sync_v2_started_at,
      metrics.datetime.raw_bookmarks_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.bookmarks_sync_incoming,
      metrics.labeled_counter.bookmarks_sync_outgoing,
      metrics.labeled_counter.bookmarks_sync_remote_tree_problems,
      metrics.labeled_counter.bookmarks_sync_v2_incoming,
      metrics.labeled_counter.bookmarks_sync_v2_outgoing,
      metrics.labeled_counter.bookmarks_sync_v2_remote_tree_problems,
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.bookmarks_sync_failure_reason,
      metrics.labeled_string.bookmarks_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.bookmarks_sync_uid,
      metrics.string.bookmarks_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.bookmarks_sync`
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
      metrics.counter.bookmarks_sync_outgoing_batches,
      metrics.counter.bookmarks_sync_v2_outgoing_batches
    ) AS `counter`,
    STRUCT(
      metrics.datetime.bookmarks_sync_finished_at,
      metrics.datetime.raw_bookmarks_sync_finished_at,
      metrics.datetime.bookmarks_sync_started_at,
      metrics.datetime.raw_bookmarks_sync_started_at,
      metrics.datetime.bookmarks_sync_v2_finished_at,
      metrics.datetime.raw_bookmarks_sync_v2_finished_at,
      metrics.datetime.bookmarks_sync_v2_started_at,
      metrics.datetime.raw_bookmarks_sync_v2_started_at
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.bookmarks_sync_incoming,
      metrics.labeled_counter.bookmarks_sync_outgoing,
      metrics.labeled_counter.bookmarks_sync_remote_tree_problems,
      metrics.labeled_counter.bookmarks_sync_v2_incoming,
      metrics.labeled_counter.bookmarks_sync_v2_outgoing,
      metrics.labeled_counter.bookmarks_sync_v2_remote_tree_problems,
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.labeled_string.bookmarks_sync_failure_reason,
      metrics.labeled_string.bookmarks_sync_v2_failure_reason
    ) AS `labeled_string`,
    STRUCT(
      metrics.string.bookmarks_sync_uid,
      metrics.string.bookmarks_sync_v2_uid,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.sync_sync_uuid, metrics.uuid.sync_v2_sync_uuid) AS `uuid`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.bookmarks_sync`