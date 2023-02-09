-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.metrics`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.glean_error_preinit_tasks_timeout,
      SAFE_CAST(NULL AS BOOLEAN) AS glean_core_migration_successful
    ) AS boolean,
    STRUCT(
      metrics.counter.glean_error_io,
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_time_invalid_timezone_offset,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_validation_foreground_count,
      SAFE_CAST(NULL AS INTEGER) AS glean_validation_app_forceclosed_count,
      SAFE_CAST(NULL AS INTEGER) AS glean_validation_baseline_ping_count
    ) AS counter,
    metrics.datetime,
    metrics.labeled_counter,
    metrics.memory_distribution,
    metrics.string
  ) AS metrics,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.mozillavpn.metrics`
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    metrics.boolean,
    metrics.counter,
    metrics.datetime,
    metrics.labeled_counter,
    STRUCT(
      STRUCT(
        metrics.memory_distribution.glean_database_size.count,
        metrics.memory_distribution.glean_database_size.sum,
        metrics.memory_distribution.glean_database_size.values
      ) AS glean_database_size,
      STRUCT(
        metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size.count,
        metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size.sum,
        metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size.values
      ) AS glean_upload_discarded_exceeding_pings_size,
      STRUCT(
        metrics.memory_distribution.glean_upload_pending_pings_directory_size.count,
        metrics.memory_distribution.glean_upload_pending_pings_directory_size.sum,
        metrics.memory_distribution.glean_upload_pending_pings_directory_size.values
      ) AS glean_upload_pending_pings_directory_size
    ) AS memory_distribution,
    metrics.string
  ) AS metrics,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.metrics`
