-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.broken_site_report`
AS
SELECT
  "org_mozilla_focus" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
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
      metrics.boolean.broken_site_report_tab_info_frameworks_mobify,
      metrics.boolean.broken_site_report_browser_info_prefs_third_party_cookie_blocking_enabled,
      metrics.boolean.broken_site_report_browser_info_prefs_third_party_cookie_blocking_enabled_in_pbm,
      metrics.boolean.broken_site_report_tab_info_antitracking_btp_has_purged_site
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
  `moz-fx-data-shared-prod.org_mozilla_focus.broken_site_report`
UNION ALL
SELECT
  "org_mozilla_focus_beta" AS normalized_app_id,
  "beta" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
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
      metrics.boolean.broken_site_report_tab_info_frameworks_mobify,
      metrics.boolean.broken_site_report_browser_info_prefs_third_party_cookie_blocking_enabled,
      metrics.boolean.broken_site_report_browser_info_prefs_third_party_cookie_blocking_enabled_in_pbm,
      metrics.boolean.broken_site_report_tab_info_antitracking_btp_has_purged_site
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
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.broken_site_report`
UNION ALL
SELECT
  "org_mozilla_focus_nightly" AS normalized_app_id,
  "nightly" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
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
      metrics.boolean.broken_site_report_tab_info_frameworks_mobify,
      metrics.boolean.broken_site_report_browser_info_prefs_third_party_cookie_blocking_enabled,
      metrics.boolean.broken_site_report_browser_info_prefs_third_party_cookie_blocking_enabled_in_pbm,
      metrics.boolean.broken_site_report_tab_info_antitracking_btp_has_purged_site
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
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.broken_site_report`
