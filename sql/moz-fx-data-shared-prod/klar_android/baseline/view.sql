-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.baseline`
AS
SELECT
  "org_mozilla_klar" AS normalized_app_id,
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
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.telemetry_sdk_build,
    client_info.build_date,
    client_info.windows_build_number,
    client_info.session_count,
    client_info.session_id
  ) AS `client_info`,
  CAST(NULL AS STRING) AS `document_id`,
  events,
  STRUCT(metadata.geo, metadata.header, metadata.isp, metadata.user_agent) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.browser_total_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_in_content,
      metrics.labeled_counter.browser_search_search_count
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.browser_default_search_engine,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(metrics.uuid.legacy_ids_client_id) AS `uuid`
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
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    ping_info.parsed_start_time,
    ping_info.parsed_end_time
  ) AS `ping_info`,
  CAST(NULL AS INTEGER) AS `sample_id`,
  CAST(NULL AS TIMESTAMP) AS `submission_timestamp`
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar.baseline`
