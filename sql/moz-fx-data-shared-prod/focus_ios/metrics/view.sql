-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_ios.metrics`
AS
SELECT
  "org_mozilla_ios_focus" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.glean_error_preinit_tasks_timeout,
      metrics.boolean.mozilla_products_has_firefox_installed,
      metrics.boolean.tracking_protection_has_advertising_blocked,
      metrics.boolean.tracking_protection_has_analytics_blocked,
      metrics.boolean.tracking_protection_has_content_blocked,
      metrics.boolean.tracking_protection_has_ever_changed_etp,
      metrics.boolean.tracking_protection_has_social_blocked
    ) AS `boolean`,
    STRUCT(
      metrics.counter.glean_error_preinit_tasks_overflow,
      metrics.counter.glean_upload_deleted_pings_after_quota_hit,
      metrics.counter.glean_upload_pending_pings,
      metrics.counter.glean_validation_foreground_count,
      metrics.counter.glean_error_io,
      metrics.counter.glean_time_invalid_timezone_offset,
      metrics.counter.settings_screen_autocomplete_domain_added,
      metrics.counter.shortcuts_shortcut_added_counter,
      metrics.counter.shortcuts_shortcut_opened_counter,
      metrics.counter.tracking_protection_toolbar_shield_clicked,
      metrics.counter.browser_total_uri_count,
      metrics.counter.settings_screen_set_as_default_browser_pressed,
      metrics.counter.app_opened_as_default_browser,
      metrics.counter.default_browser_onboarding_dismiss_pressed,
      metrics.counter.default_browser_onboarding_go_to_settings_pressed,
      metrics.counter.browser_pdf_viewer_used,
      metrics.counter.glean_upload_in_flight_pings_dropped,
      metrics.counter.glean_upload_missing_send_ids
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_upload_ping_upload_failure,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.shortcuts_shortcut_removed_counter,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.browser_search_search_count,
      metrics.labeled_counter.browser_search_in_content
    ) AS `labeled_counter`,
    STRUCT(
      metrics.memory_distribution.glean_database_size,
      metrics.memory_distribution.glean_upload_discarded_exceeding_pings_size,
      metrics.memory_distribution.glean_upload_pending_pings_directory_size
    ) AS `memory_distribution`,
    STRUCT(
      metrics.string.ping_reason,
      metrics.string.preferences_user_theme,
      metrics.string.search_default_engine,
      metrics.string.app_keyboard_type,
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.glean_database_rkv_load_error
    ) AS `string`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(metrics.quantity.shortcuts_shortcuts_on_home_number) AS `quantity`,
    STRUCT(
      metrics.timing_distribution.nimbus_health_apply_pending_experiments_time,
      metrics.timing_distribution.nimbus_health_fetch_experiments_time,
      metrics.timing_distribution.glean_upload_send_failure,
      metrics.timing_distribution.glean_upload_send_success,
      metrics.timing_distribution.glean_validation_shutdown_wait,
      metrics.timing_distribution.glean_validation_shutdown_dispatcher_wait,
      metrics.timing_distribution.glean_database_write_time
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
  `moz-fx-data-shared-prod.org_mozilla_ios_focus.metrics`
