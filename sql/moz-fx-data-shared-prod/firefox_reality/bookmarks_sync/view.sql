-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality.bookmarks_sync`
AS
SELECT
  "org_mozilla_vrbrowser" AS normalized_app_id,
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
    client_info.os,
    client_info.os_version,
    client_info.telemetry_sdk_build,
    client_info.locale,
    client_info.build_date,
    client_info.windows_build_number,
    client_info.session_count,
    client_info.session_id
  ) AS `client_info`,
  CAST(NULL AS STRING) AS `document_id`,
  events,
  STRUCT(metadata.geo, metadata.header, metadata.user_agent, metadata.isp) AS `metadata`,
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
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.bookmarks_sync_v2_incoming,
      metrics.labeled_counter.bookmarks_sync_v2_outgoing,
      metrics.labeled_counter.bookmarks_sync_v2_remote_tree_problems
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
  CAST(NULL AS STRING) AS `normalized_app_name`,
  CAST(NULL AS STRING) AS `normalized_channel`,
  CAST(NULL AS STRING) AS `normalized_country_code`,
  CAST(NULL AS STRING) AS `normalized_os`,
  CAST(NULL AS STRING) AS `normalized_os_version`,
  STRUCT(
    ping_info.end_time,
    ping_info.experiments,
    ping_info.ping_type,
    ping_info.seq,
    ping_info.start_time,
    ping_info.reason,
    ping_info.parsed_start_time,
    ping_info.parsed_end_time
  ) AS `ping_info`,
  CAST(NULL AS INTEGER) AS `sample_id`,
  CAST(NULL AS TIMESTAMP) AS `submission_timestamp`
FROM
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser.bookmarks_sync`
