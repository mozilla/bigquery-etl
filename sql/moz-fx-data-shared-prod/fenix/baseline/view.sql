-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.baseline`
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
      metrics.counter.events_total_uri_count,
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.events_normal_and_private_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.metrics_search_count,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_in_content,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.search_default_engine_code,
      metrics.string.search_default_engine_name,
      metrics.string.metrics_distribution_id,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.baseline`
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
      metrics.counter.events_total_uri_count,
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.events_normal_and_private_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.metrics_search_count,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_in_content,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.search_default_engine_code,
      metrics.string.search_default_engine_name,
      metrics.string.metrics_distribution_id,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
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
    client_info.session_id,
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.events_total_uri_count,
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.events_normal_and_private_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.metrics_search_count,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_in_content,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.search_default_engine_code,
      metrics.string.search_default_engine_name,
      metrics.string.metrics_distribution_id,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
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
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.baseline`
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
      metrics.counter.events_total_uri_count,
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.events_normal_and_private_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.metrics_search_count,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_in_content,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.glean_validation_pings_submitted
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.search_default_engine_code,
      metrics.string.search_default_engine_name,
      metrics.string.metrics_distribution_id,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.first_session_install_source
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(metrics.boolean.metrics_default_browser) AS `boolean`
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.baseline`
