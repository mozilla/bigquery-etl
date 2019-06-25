WITH
  -- normalize client_id and rank by document_id
  numbered_duplicates AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id, submission_date_s3, document_id ORDER BY `timestamp` ASC) AS _n,
    * REPLACE(LOWER(client_id) AS client_id)
  FROM
    main_summary_v4
  WHERE
    submission_date_s3 = @submission_date
    AND client_id IS NOT NULL ),
  -- Deduplicating on document_id is necessary to get valid SUM values.
  deduplicated AS (
  SELECT
    * EXCEPT (_n)
  FROM
    numbered_duplicates
  WHERE
    _n = 1 ),
  -- Aggregate by client_id using windows
  windowed AS (
  SELECT
    ROW_NUMBER() OVER w1_unframed AS _n,
    client_id,
    SUM(aborts_content) OVER w1 AS aborts_content_sum,
    SUM(aborts_gmplugin) OVER w1 AS aborts_gmplugin_sum,
    SUM(aborts_plugin) OVER w1 AS aborts_plugin_sum,
    AVG(active_addons_count) OVER w1 AS active_addons_count_mean,
    udf_aggregate_active_addons(ARRAY_AGG(active_addons) OVER w1) AS active_addons,
    CAST(NULL AS STRING) AS active_experiment_branch, -- deprecated
    CAST(NULL AS STRING) AS active_experiment_id, -- deprecated
    SUM(active_ticks/(3600/5)) OVER w1 AS active_hours_sum,
    FIRST_VALUE(addon_compatibility_check_enabled IGNORE NULLS) OVER w1 AS addon_compatibility_check_enabled,
    FIRST_VALUE(app_build_id IGNORE NULLS) OVER w1 AS app_build_id,
    FIRST_VALUE(app_display_version IGNORE NULLS) OVER w1 AS app_display_version,
    FIRST_VALUE(app_name IGNORE NULLS) OVER w1 AS app_name,
    FIRST_VALUE(app_version IGNORE NULLS) OVER w1 AS app_version,
    FIRST_VALUE(attribution IGNORE NULLS) OVER w1 AS attribution,
    FIRST_VALUE(blocklist_enabled IGNORE NULLS) OVER w1 AS blocklist_enabled,
    FIRST_VALUE(channel IGNORE NULLS) OVER w1 AS channel,
    AVG(client_clock_skew) OVER w1 AS client_clock_skew_mean,
    AVG(client_submission_latency) OVER w1 AS client_submission_latency_mean,
    FIRST_VALUE(cpu_cores IGNORE NULLS) OVER w1 AS cpu_cores,
    FIRST_VALUE(cpu_count IGNORE NULLS) OVER w1 AS cpu_count,
    FIRST_VALUE(cpu_family IGNORE NULLS) OVER w1 AS cpu_family,
    FIRST_VALUE(cpu_l2_cache_kb IGNORE NULLS) OVER w1 AS cpu_l2_cache_kb,
    FIRST_VALUE(cpu_l3_cache_kb IGNORE NULLS) OVER w1 AS cpu_l3_cache_kb,
    FIRST_VALUE(cpu_model IGNORE NULLS) OVER w1 AS cpu_model,
    FIRST_VALUE(cpu_speed_mhz IGNORE NULLS) OVER w1 AS cpu_speed_mhz,
    FIRST_VALUE(cpu_stepping IGNORE NULLS) OVER w1 AS cpu_stepping,
    FIRST_VALUE(cpu_vendor IGNORE NULLS) OVER w1 AS cpu_vendor,
    SUM(crashes_detected_content) OVER w1 AS crashes_detected_content_sum,
    SUM(crashes_detected_gmplugin) OVER w1 AS crashes_detected_gmplugin_sum,
    SUM(crashes_detected_plugin) OVER w1 AS crashes_detected_plugin_sum,
    SUM(crash_submit_attempt_content) OVER w1 AS crash_submit_attempt_content_sum,
    SUM(crash_submit_attempt_main) OVER w1 AS crash_submit_attempt_main_sum,
    SUM(crash_submit_attempt_plugin) OVER w1 AS crash_submit_attempt_plugin_sum,
    SUM(crash_submit_success_content) OVER w1 AS crash_submit_success_content_sum,
    SUM(crash_submit_success_main) OVER w1 AS crash_submit_success_main_sum,
    SUM(crash_submit_success_plugin) OVER w1 AS crash_submit_success_plugin_sum,
    FIRST_VALUE(default_search_engine IGNORE NULLS) OVER w1 AS default_search_engine,
    FIRST_VALUE(default_search_engine_data_load_path IGNORE NULLS) OVER w1 AS default_search_engine_data_load_path,
    FIRST_VALUE(default_search_engine_data_name IGNORE NULLS) OVER w1 AS default_search_engine_data_name,
    FIRST_VALUE(default_search_engine_data_origin IGNORE NULLS) OVER w1 AS default_search_engine_data_origin,
    FIRST_VALUE(default_search_engine_data_submission_url IGNORE NULLS) OVER w1 AS default_search_engine_data_submission_url,
    SUM(devtools_toolbox_opened_count) OVER w1 AS devtools_toolbox_opened_count_sum,
    FIRST_VALUE(distribution_id IGNORE NULLS) OVER w1 AS distribution_id,
    FIRST_VALUE(e10s_enabled IGNORE NULLS) OVER w1 AS e10s_enabled,
    FIRST_VALUE(env_build_arch IGNORE NULLS) OVER w1 AS env_build_arch,
    FIRST_VALUE(env_build_id IGNORE NULLS) OVER w1 AS env_build_id,
    FIRST_VALUE(env_build_version IGNORE NULLS) OVER w1 AS env_build_version,
    FIRST_VALUE(udf_null_if_empty_list(environment_settings_intl_accept_languages) IGNORE NULLS) OVER w1 AS environment_settings_intl_accept_languages,
    FIRST_VALUE(udf_null_if_empty_list(environment_settings_intl_app_locales) IGNORE NULLS) OVER w1 AS environment_settings_intl_app_locales,
    FIRST_VALUE(udf_null_if_empty_list(environment_settings_intl_available_locales) IGNORE NULLS) OVER w1 AS environment_settings_intl_available_locales,
    FIRST_VALUE(udf_null_if_empty_list(environment_settings_intl_requested_locales) IGNORE NULLS) OVER w1 AS environment_settings_intl_requested_locales,
    FIRST_VALUE(udf_null_if_empty_list(environment_settings_intl_system_locales) IGNORE NULLS) OVER w1 AS environment_settings_intl_system_locales,
    FIRST_VALUE(udf_null_if_empty_list(environment_settings_intl_regional_prefs_locales) IGNORE NULLS) OVER w1 AS environment_settings_intl_regional_prefs_locales,
    udf_aggregate_map_first(ARRAY_AGG(experiments) OVER w1) AS experiments,
    AVG(first_paint) OVER w1 AS first_paint_mean,
    FIRST_VALUE(flash_version IGNORE NULLS) OVER w1 AS flash_version,
    FIRST_VALUE(udf_geo_struct(country, city, geo_subdivision1, geo_subdivision2) IGNORE NULLS) OVER w1.*,
    FIRST_VALUE(gfx_features_advanced_layers_status IGNORE NULLS) OVER w1 AS gfx_features_advanced_layers_status,
    FIRST_VALUE(gfx_features_d2d_status IGNORE NULLS) OVER w1 AS gfx_features_d2d_status,
    FIRST_VALUE(gfx_features_d3d11_status IGNORE NULLS) OVER w1 AS gfx_features_d3d11_status,
    FIRST_VALUE(gfx_features_gpu_process_status IGNORE NULLS) OVER w1 AS gfx_features_gpu_process_status,
    SUM(histogram_parent_devtools_aboutdebugging_opened_count) OVER w1 AS histogram_parent_devtools_aboutdebugging_opened_count_sum,
    SUM(histogram_parent_devtools_animationinspector_opened_count) OVER w1 AS histogram_parent_devtools_animationinspector_opened_count_sum,
    SUM(histogram_parent_devtools_browserconsole_opened_count) OVER w1 AS histogram_parent_devtools_browserconsole_opened_count_sum,
    SUM(histogram_parent_devtools_canvasdebugger_opened_count) OVER w1 AS histogram_parent_devtools_canvasdebugger_opened_count_sum,
    SUM(histogram_parent_devtools_computedview_opened_count) OVER w1 AS histogram_parent_devtools_computedview_opened_count_sum,
    SUM(histogram_parent_devtools_custom_opened_count) OVER w1 AS histogram_parent_devtools_custom_opened_count_sum,
    NULL AS histogram_parent_devtools_developertoolbar_opened_count_sum, -- deprecated
    SUM(histogram_parent_devtools_dom_opened_count) OVER w1 AS histogram_parent_devtools_dom_opened_count_sum,
    SUM(histogram_parent_devtools_eyedropper_opened_count) OVER w1 AS histogram_parent_devtools_eyedropper_opened_count_sum,
    SUM(histogram_parent_devtools_fontinspector_opened_count) OVER w1 AS histogram_parent_devtools_fontinspector_opened_count_sum,
    SUM(histogram_parent_devtools_inspector_opened_count) OVER w1 AS histogram_parent_devtools_inspector_opened_count_sum,
    SUM(histogram_parent_devtools_jsbrowserdebugger_opened_count) OVER w1 AS histogram_parent_devtools_jsbrowserdebugger_opened_count_sum,
    SUM(histogram_parent_devtools_jsdebugger_opened_count) OVER w1 AS histogram_parent_devtools_jsdebugger_opened_count_sum,
    SUM(histogram_parent_devtools_jsprofiler_opened_count) OVER w1 AS histogram_parent_devtools_jsprofiler_opened_count_sum,
    SUM(histogram_parent_devtools_layoutview_opened_count) OVER w1 AS histogram_parent_devtools_layoutview_opened_count_sum,
    SUM(histogram_parent_devtools_memory_opened_count) OVER w1 AS histogram_parent_devtools_memory_opened_count_sum,
    SUM(histogram_parent_devtools_menu_eyedropper_opened_count) OVER w1 AS histogram_parent_devtools_menu_eyedropper_opened_count_sum,
    SUM(histogram_parent_devtools_netmonitor_opened_count) OVER w1 AS histogram_parent_devtools_netmonitor_opened_count_sum,
    SUM(histogram_parent_devtools_options_opened_count) OVER w1 AS histogram_parent_devtools_options_opened_count_sum,
    SUM(histogram_parent_devtools_paintflashing_opened_count) OVER w1 AS histogram_parent_devtools_paintflashing_opened_count_sum,
    SUM(histogram_parent_devtools_picker_eyedropper_opened_count) OVER w1 AS histogram_parent_devtools_picker_eyedropper_opened_count_sum,
    SUM(histogram_parent_devtools_responsive_opened_count) OVER w1 AS histogram_parent_devtools_responsive_opened_count_sum,
    SUM(histogram_parent_devtools_ruleview_opened_count) OVER w1 AS histogram_parent_devtools_ruleview_opened_count_sum,
    SUM(histogram_parent_devtools_scratchpad_opened_count) OVER w1 AS histogram_parent_devtools_scratchpad_opened_count_sum,
    SUM(histogram_parent_devtools_scratchpad_window_opened_count) OVER w1 AS histogram_parent_devtools_scratchpad_window_opened_count_sum,
    SUM(histogram_parent_devtools_shadereditor_opened_count) OVER w1 AS histogram_parent_devtools_shadereditor_opened_count_sum,
    SUM(histogram_parent_devtools_storage_opened_count) OVER w1 AS histogram_parent_devtools_storage_opened_count_sum,
    SUM(histogram_parent_devtools_styleeditor_opened_count) OVER w1 AS histogram_parent_devtools_styleeditor_opened_count_sum,
    SUM(histogram_parent_devtools_webaudioeditor_opened_count) OVER w1 AS histogram_parent_devtools_webaudioeditor_opened_count_sum,
    SUM(histogram_parent_devtools_webconsole_opened_count) OVER w1 AS histogram_parent_devtools_webconsole_opened_count_sum,
    SUM(histogram_parent_devtools_webide_opened_count) OVER w1 AS histogram_parent_devtools_webide_opened_count_sum,
    FIRST_VALUE(install_year IGNORE NULLS) OVER w1 AS install_year,
    FIRST_VALUE(is_default_browser IGNORE NULLS) OVER w1 AS is_default_browser,
    FIRST_VALUE(is_wow64 IGNORE NULLS) OVER w1 AS is_wow64,
    FIRST_VALUE(locale IGNORE NULLS) OVER w1 AS locale,
    FIRST_VALUE(memory_mb IGNORE NULLS) OVER w1 AS memory_mb,
    FIRST_VALUE(normalized_channel IGNORE NULLS) OVER w1 AS normalized_channel,
    FIRST_VALUE(normalized_os_version IGNORE NULLS) OVER w1 AS normalized_os_version,
    FIRST_VALUE(os IGNORE NULLS) OVER w1 AS os,
    FIRST_VALUE(os_service_pack_major IGNORE NULLS) OVER w1 AS os_service_pack_major,
    FIRST_VALUE(os_service_pack_minor IGNORE NULLS) OVER w1 AS os_service_pack_minor,
    FIRST_VALUE(os_version IGNORE NULLS) OVER w1 AS os_version,
    COUNT(*) OVER w1 AS pings_aggregated_by_this_row,
    AVG(places_bookmarks_count) OVER w1 AS places_bookmarks_count_mean,
    AVG(places_pages_count) OVER w1 AS places_pages_count_mean,
    SUM(plugin_hangs) OVER w1 AS plugin_hangs_sum,
    SUM(plugins_infobar_allow) OVER w1 AS plugins_infobar_allow_sum,
    SUM(plugins_infobar_block) OVER w1 AS plugins_infobar_block_sum,
    SUM(plugins_infobar_shown) OVER w1 AS plugins_infobar_shown_sum,
    SUM(plugins_notification_shown) OVER w1 AS plugins_notification_shown_sum,
    FIRST_VALUE(previous_build_id IGNORE NULLS) OVER w1 AS previous_build_id,
    UNIX_DATE(DATE(SAFE.TIMESTAMP(subsession_start_date))) - profile_creation_date AS profile_age_in_days,
    SAFE.DATE_FROM_UNIX_DATE(profile_creation_date) AS profile_creation_date,
    SUM(push_api_notify) OVER w1 AS push_api_notify_sum,
    FIRST_VALUE(sample_id IGNORE NULLS) OVER w1 AS sample_id,
    FIRST_VALUE(sandbox_effective_content_process_level IGNORE NULLS) OVER w1 AS sandbox_effective_content_process_level,
    SUM(scalar_parent_webrtc_nicer_stun_retransmits + scalar_content_webrtc_nicer_stun_retransmits) OVER w1 AS scalar_combined_webrtc_nicer_stun_retransmits_sum,
    SUM(scalar_parent_webrtc_nicer_turn_401s + scalar_content_webrtc_nicer_turn_401s) OVER w1 AS scalar_combined_webrtc_nicer_turn_401s_sum,
    SUM(scalar_parent_webrtc_nicer_turn_403s + scalar_content_webrtc_nicer_turn_403s) OVER w1 AS scalar_combined_webrtc_nicer_turn_403s_sum,
    SUM(scalar_parent_webrtc_nicer_turn_438s + scalar_content_webrtc_nicer_turn_438s) OVER w1 AS scalar_combined_webrtc_nicer_turn_438s_sum,
    SUM(scalar_content_navigator_storage_estimate_count) OVER w1 AS scalar_content_navigator_storage_estimate_count_sum,
    SUM(scalar_content_navigator_storage_persist_count) OVER w1 AS scalar_content_navigator_storage_persist_count_sum,
    FIRST_VALUE(scalar_parent_aushelper_websense_reg_version IGNORE NULLS) OVER w1 AS scalar_parent_aushelper_websense_reg_version,
    MAX(scalar_parent_browser_engagement_max_concurrent_tab_count) OVER w1 AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
    MAX(scalar_parent_browser_engagement_max_concurrent_window_count) OVER w1 AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
    SUM(scalar_parent_browser_engagement_tab_open_event_count) OVER w1 AS scalar_parent_browser_engagement_tab_open_event_count_sum,
    SUM(scalar_parent_browser_engagement_total_uri_count) OVER w1 AS scalar_parent_browser_engagement_total_uri_count_sum,
    SUM(scalar_parent_browser_engagement_unfiltered_uri_count) OVER w1 AS scalar_parent_browser_engagement_unfiltered_uri_count_sum,
    MAX(scalar_parent_browser_engagement_unique_domains_count) OVER w1 AS scalar_parent_browser_engagement_unique_domains_count_max,
    AVG(scalar_parent_browser_engagement_unique_domains_count) OVER w1 AS scalar_parent_browser_engagement_unique_domains_count_mean,
    SUM(scalar_parent_browser_engagement_window_open_event_count) OVER w1 AS scalar_parent_browser_engagement_window_open_event_count_sum,
    SUM(scalar_parent_devtools_accessibility_node_inspected_count) OVER w1 AS scalar_parent_devtools_accessibility_node_inspected_count_sum,
    SUM(scalar_parent_devtools_accessibility_opened_count) OVER w1 AS scalar_parent_devtools_accessibility_opened_count_sum,
    SUM(scalar_parent_devtools_accessibility_picker_used_count) OVER w1 AS scalar_parent_devtools_accessibility_picker_used_count_sum,
    udf_aggregate_map_sum(ARRAY_AGG(scalar_parent_devtools_accessibility_select_accessible_for_node) OVER w1) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum,
    SUM(scalar_parent_devtools_accessibility_service_enabled_count) OVER w1 AS scalar_parent_devtools_accessibility_service_enabled_count_sum,
    SUM(scalar_parent_devtools_copy_full_css_selector_opened) OVER w1 AS scalar_parent_devtools_copy_full_css_selector_opened_sum,
    SUM(scalar_parent_devtools_copy_unique_css_selector_opened) OVER w1 AS scalar_parent_devtools_copy_unique_css_selector_opened_sum,
    SUM(scalar_parent_devtools_toolbar_eyedropper_opened) OVER w1 AS scalar_parent_devtools_toolbar_eyedropper_opened_sum,
    NULL AS scalar_parent_dom_contentprocess_troubled_due_to_memory_sum, -- deprecated
    SUM(scalar_parent_navigator_storage_estimate_count) OVER w1 AS scalar_parent_navigator_storage_estimate_count_sum,
    SUM(scalar_parent_navigator_storage_persist_count) OVER w1 AS scalar_parent_navigator_storage_persist_count_sum,
    SUM(scalar_parent_storage_sync_api_usage_extensions_using) OVER w1 AS scalar_parent_storage_sync_api_usage_extensions_using_sum,
    FIRST_VALUE(search_cohort IGNORE NULLS) OVER w1 AS search_cohort,
    udf_aggregate_search_counts(ARRAY_AGG(search_counts) OVER w1).*,
    AVG(session_restored) OVER w1 AS session_restored_mean,
    COUNTIF(subsession_counter = 1) OVER w1 AS sessions_started_on_this_day,
    SUM(shutdown_kill) OVER w1 AS shutdown_kill_sum,
    SUM(subsession_length/NUMERIC '3600') OVER w1 AS subsession_hours_sum,
    SUM(ssl_handshake_result_failure) OVER w1 AS ssl_handshake_result_failure_sum,
    SUM(ssl_handshake_result_success) OVER w1 AS ssl_handshake_result_success_sum,
    FIRST_VALUE(sync_configured IGNORE NULLS) OVER w1 AS sync_configured,
    AVG(sync_count_desktop) OVER w1 AS sync_count_desktop_mean,
    AVG(sync_count_mobile) OVER w1 AS sync_count_mobile_mean,
    SUM(sync_count_desktop) OVER w1 AS sync_count_desktop_sum,
    SUM(sync_count_mobile) OVER w1 AS sync_count_mobile_sum,
    FIRST_VALUE(telemetry_enabled IGNORE NULLS) OVER w1 AS telemetry_enabled,
    FIRST_VALUE(timezone_offset IGNORE NULLS) OVER w1 AS timezone_offset,
    CAST(NULL AS NUMERIC) AS total_hours_sum,
    FIRST_VALUE(update_auto_download IGNORE NULLS) OVER w1 AS update_auto_download,
    FIRST_VALUE(update_channel IGNORE NULLS) OVER w1 AS update_channel,
    FIRST_VALUE(update_enabled IGNORE NULLS) OVER w1 AS update_enabled,
    FIRST_VALUE(vendor IGNORE NULLS) OVER w1 AS vendor,
    SUM(web_notification_shown) OVER w1 AS web_notification_shown_sum,
    FIRST_VALUE(windows_build_number IGNORE NULLS) OVER w1 AS windows_build_number,
    FIRST_VALUE(windows_ubr IGNORE NULLS) OVER w1 AS windows_ubr
  FROM
    deduplicated
  WINDOW
    -- Aggregations require a framed window
    w1 AS (
    PARTITION BY
      client_id,
      submission_date_s3
    ORDER BY
      `timestamp` ASC ROWS BETWEEN UNBOUNDED PRECEDING
      AND UNBOUNDED FOLLOWING),
    -- ROW_NUMBER does not work on a framed window
    w1_unframed AS (
    PARTITION BY
      client_id,
      submission_date_s3
    ORDER BY
      `timestamp` ASC) )
SELECT
  @submission_date AS submission_date,
  * EXCEPT(_n)
FROM
  windowed
WHERE
  _n = 1
