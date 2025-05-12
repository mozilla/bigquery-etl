-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.daemonsession`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.counter.session_connection_health_stable_count,
      metrics.counter.connection_health_no_signal_count,
      metrics.counter.connection_health_stable_count,
      metrics.counter.connection_health_unstable_count,
      metrics.counter.connection_health_pending_count
    ) AS `counter`,
    STRUCT(
      metrics.datetime.session_daemon_session_end,
      metrics.datetime.raw_session_daemon_session_end,
      metrics.datetime.session_daemon_session_start,
      metrics.datetime.raw_session_daemon_session_start
    ) AS `datetime`,
    STRUCT(
      metrics.string.session_daemon_session_source,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.session_daemon_session_id, metrics.uuid.session_installation_id) AS `uuid`,
    STRUCT(
      metrics.timing_distribution.connection_health_no_signal_time,
      metrics.timing_distribution.connection_health_stable_time,
      metrics.timing_distribution.connection_health_unstable_time,
      metrics.timing_distribution.connection_health_pending_time
    ) AS `timing_distribution`,
    STRUCT(
      metrics.custom_distribution.connection_health_data_transferred_rx,
      metrics.custom_distribution.connection_health_data_transferred_tx
    ) AS `custom_distribution`
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
  `moz-fx-data-shared-prod.mozillavpn.daemonsession`
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.counter.session_connection_health_stable_count,
      metrics.counter.connection_health_no_signal_count,
      metrics.counter.connection_health_stable_count,
      metrics.counter.connection_health_unstable_count,
      metrics.counter.connection_health_pending_count
    ) AS `counter`,
    STRUCT(
      metrics.datetime.session_daemon_session_end,
      metrics.datetime.raw_session_daemon_session_end,
      metrics.datetime.session_daemon_session_start,
      metrics.datetime.raw_session_daemon_session_start
    ) AS `datetime`,
    STRUCT(
      metrics.string.session_daemon_session_source,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.session_daemon_session_id, metrics.uuid.session_installation_id) AS `uuid`,
    STRUCT(
      metrics.timing_distribution.connection_health_no_signal_time,
      metrics.timing_distribution.connection_health_stable_time,
      metrics.timing_distribution.connection_health_unstable_time,
      metrics.timing_distribution.connection_health_pending_time
    ) AS `timing_distribution`,
    STRUCT(
      metrics.custom_distribution.connection_health_data_transferred_rx,
      metrics.custom_distribution.connection_health_data_transferred_tx
    ) AS `custom_distribution`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.daemonsession`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.counter.session_connection_health_stable_count,
      metrics.counter.connection_health_no_signal_count,
      metrics.counter.connection_health_stable_count,
      metrics.counter.connection_health_unstable_count,
      metrics.counter.connection_health_pending_count
    ) AS `counter`,
    STRUCT(
      metrics.datetime.session_daemon_session_end,
      metrics.datetime.raw_session_daemon_session_end,
      metrics.datetime.session_daemon_session_start,
      metrics.datetime.raw_session_daemon_session_start
    ) AS `datetime`,
    STRUCT(
      metrics.string.session_daemon_session_source,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.session_daemon_session_id, metrics.uuid.session_installation_id) AS `uuid`,
    STRUCT(
      metrics.timing_distribution.connection_health_no_signal_time,
      metrics.timing_distribution.connection_health_stable_time,
      metrics.timing_distribution.connection_health_unstable_time,
      metrics.timing_distribution.connection_health_pending_time
    ) AS `timing_distribution`,
    STRUCT(
      metrics.custom_distribution.connection_health_data_transferred_rx,
      metrics.custom_distribution.connection_health_data_transferred_tx
    ) AS `custom_distribution`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn.daemonsession`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn_network_extension" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.counter.session_connection_health_stable_count,
      metrics.counter.connection_health_no_signal_count,
      metrics.counter.connection_health_stable_count,
      metrics.counter.connection_health_unstable_count,
      metrics.counter.connection_health_pending_count
    ) AS `counter`,
    STRUCT(
      metrics.datetime.session_daemon_session_end,
      metrics.datetime.raw_session_daemon_session_end,
      metrics.datetime.session_daemon_session_start,
      metrics.datetime.raw_session_daemon_session_start
    ) AS `datetime`,
    STRUCT(
      metrics.string.session_daemon_session_source,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.uuid.session_daemon_session_id, metrics.uuid.session_installation_id) AS `uuid`,
    STRUCT(
      metrics.timing_distribution.connection_health_no_signal_time,
      metrics.timing_distribution.connection_health_stable_time,
      metrics.timing_distribution.connection_health_unstable_time,
      metrics.timing_distribution.connection_health_pending_time
    ) AS `timing_distribution`,
    STRUCT(
      metrics.custom_distribution.connection_health_data_transferred_rx,
      metrics.custom_distribution.connection_health_data_transferred_tx
    ) AS `custom_distribution`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.daemonsession`
