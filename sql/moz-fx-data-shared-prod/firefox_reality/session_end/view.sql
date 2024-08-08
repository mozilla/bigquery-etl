-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality.session_end`
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
      metrics.counter.tabs_activated,
      metrics.counter.url_domains,
      metrics.counter.url_visits,
      metrics.counter.control_open_new_window,
      metrics.counter.windows_movement,
      metrics.counter.windows_resize
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.tabs_opened,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.windows_opened_pri_window_count,
      metrics.labeled_counter.windows_opened_window_count
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.distribution_channel_name,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.windows_active_in_front_time,
      metrics.timing_distribution.windows_active_in_left_time,
      metrics.timing_distribution.windows_active_in_right_time,
      metrics.timing_distribution.windows_double_pri_window_opened_time,
      metrics.timing_distribution.windows_double_window_opened_time,
      metrics.timing_distribution.windows_single_pri_window_opened_time,
      metrics.timing_distribution.windows_single_window_opened_time,
      metrics.timing_distribution.windows_triple_pri_window_opened_time,
      metrics.timing_distribution.windows_triple_window_opened_time
    ) AS `timing_distribution`
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
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser.session_end`
