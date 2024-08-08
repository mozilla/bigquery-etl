-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.broken_site_report`
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
    client_info.build_date,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.telemetry_sdk_build,
    client_info.windows_build_number,
    client_info.session_count,
    client_info.session_id
  ) AS `client_info`,
  CAST(NULL AS STRING) AS `document_id`,
  events,
  STRUCT(metadata.geo, metadata.header, metadata.isp, metadata.user_agent) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.boolean.broken_site_report_browser_info_app_fission_enabled,
      metrics.boolean.broken_site_report_browser_info_graphics_has_touch_screen,
      metrics.boolean.broken_site_report_browser_info_prefs_forced_accelerated_layers,
      metrics.boolean.broken_site_report_browser_info_prefs_global_privacy_control_enabled,
      metrics.boolean.broken_site_report_browser_info_prefs_installtrigger_enabled,
      metrics.boolean.broken_site_report_browser_info_prefs_opaque_response_blocking,
      metrics.boolean.broken_site_report_browser_info_prefs_resist_fingerprinting_enabled,
      metrics.boolean.broken_site_report_browser_info_prefs_software_webrender,
      metrics.boolean.broken_site_report_browser_info_system_is_tablet,
      metrics.boolean.broken_site_report_tab_info_antitracking_has_mixed_active_content_blocked,
      metrics.boolean.broken_site_report_tab_info_antitracking_has_mixed_display_content_blocked,
      metrics.boolean.broken_site_report_tab_info_antitracking_has_tracking_content_blocked,
      metrics.boolean.broken_site_report_tab_info_antitracking_is_private_browsing,
      metrics.boolean.broken_site_report_tab_info_frameworks_fastclick,
      metrics.boolean.broken_site_report_tab_info_frameworks_marfeel,
      metrics.boolean.broken_site_report_tab_info_frameworks_mobify
    ) AS `boolean`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.broken_site_report_browser_info_prefs_cookie_behavior,
      metrics.quantity.broken_site_report_browser_info_system_memory
    ) AS `quantity`,
    STRUCT(
      metrics.string.broken_site_report_breakage_category,
      metrics.string.broken_site_report_browser_info_graphics_device_pixel_ratio,
      metrics.string.broken_site_report_tab_info_antitracking_block_list,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.string_list.broken_site_report_browser_info_app_default_locales,
      metrics.string_list.broken_site_report_browser_info_security_antispyware,
      metrics.string_list.broken_site_report_browser_info_security_antivirus,
      metrics.string_list.broken_site_report_browser_info_security_firewall,
      metrics.string_list.broken_site_report_tab_info_languages
    ) AS `string_list`,
    STRUCT(
      metrics.text2.broken_site_report_browser_info_app_default_useragent_string,
      metrics.text2.broken_site_report_browser_info_graphics_devices_json,
      metrics.text2.broken_site_report_browser_info_graphics_drivers_json,
      metrics.text2.broken_site_report_browser_info_graphics_features_json,
      metrics.text2.broken_site_report_browser_info_graphics_monitors_json,
      metrics.text2.broken_site_report_description,
      metrics.text2.broken_site_report_tab_info_useragent_string
    ) AS `text2`,
    STRUCT(metrics.url2.broken_site_report_url) AS `url2`,
    STRUCT(
      metrics.text.broken_site_report_browser_info_app_default_useragent_string,
      metrics.text.broken_site_report_browser_info_graphics_devices_json,
      metrics.text.broken_site_report_browser_info_graphics_drivers_json,
      metrics.text.broken_site_report_browser_info_graphics_features_json,
      metrics.text.broken_site_report_browser_info_graphics_monitors_json,
      metrics.text.broken_site_report_description,
      metrics.text.broken_site_report_tab_info_useragent_string
    ) AS `text`,
    STRUCT(metrics.url.broken_site_report_url) AS `url`
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
  `moz-fx-data-shared-prod.org_mozilla_klar.broken_site_report`
