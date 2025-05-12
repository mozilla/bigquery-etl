-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_reality.session_end`
AS
SELECT
  "org_mozilla_vrbrowser" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
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
  `moz-fx-data-shared-prod.org_mozilla_vrbrowser.session_end`
