-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.lockwise_android.tabs_sync`
AS
SELECT
  "mozilla_lockbox" AS normalized_app_id,
  normalized_channel,
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
  `moz-fx-data-shared-prod.mozilla_lockbox.tabs_sync`