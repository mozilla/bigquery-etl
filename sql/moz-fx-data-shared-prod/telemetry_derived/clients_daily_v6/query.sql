WITH overactive AS (
  -- find client_ids with over 200,000 pings in a day
  SELECT
    client_id
  FROM
    main_summary_v4
  WHERE
    submission_date = @submission_date
  GROUP BY
    client_id
  HAVING
    COUNT(*) > 200000
)
SELECT
  submission_date,
  client_id,
  SUM(aborts_content) AS aborts_content_sum,
  SUM(aborts_gmplugin) AS aborts_gmplugin_sum,
  SUM(aborts_plugin) AS aborts_plugin_sum,
  AVG(active_addons_count) AS active_addons_count_mean,
  udf.aggregate_active_addons(
    ARRAY_CONCAT_AGG(active_addons ORDER BY `timestamp`)
  ) AS active_addons,
  CAST(
    NULL AS STRING
  ) AS active_experiment_branch, -- deprecated
  CAST(
    NULL AS STRING
  ) AS active_experiment_id, -- deprecated
  SUM(active_ticks / (3600 / 5)) AS active_hours_sum,
  udf.mode_last(
    ARRAY_AGG(addon_compatibility_check_enabled ORDER BY `timestamp`)
  ) AS addon_compatibility_check_enabled,
  udf.mode_last(ARRAY_AGG(app_build_id ORDER BY `timestamp`)) AS app_build_id,
  udf.mode_last(ARRAY_AGG(app_display_version ORDER BY `timestamp`)) AS app_display_version,
  udf.mode_last(ARRAY_AGG(app_name ORDER BY `timestamp`)) AS app_name,
  udf.mode_last(ARRAY_AGG(app_version ORDER BY `timestamp`)) AS app_version,
  udf.json_mode_last(ARRAY_AGG(attribution ORDER BY `timestamp`)) AS attribution,
  udf.mode_last(ARRAY_AGG(blocklist_enabled ORDER BY `timestamp`)) AS blocklist_enabled,
  udf.mode_last(ARRAY_AGG(channel ORDER BY `timestamp`)) AS channel,
  AVG(client_clock_skew) AS client_clock_skew_mean,
  AVG(client_submission_latency) AS client_submission_latency_mean,
  udf.mode_last(ARRAY_AGG(cpu_cores ORDER BY `timestamp`)) AS cpu_cores,
  udf.mode_last(ARRAY_AGG(cpu_count ORDER BY `timestamp`)) AS cpu_count,
  udf.mode_last(ARRAY_AGG(cpu_family ORDER BY `timestamp`)) AS cpu_family,
  udf.mode_last(ARRAY_AGG(cpu_l2_cache_kb ORDER BY `timestamp`)) AS cpu_l2_cache_kb,
  udf.mode_last(ARRAY_AGG(cpu_l3_cache_kb ORDER BY `timestamp`)) AS cpu_l3_cache_kb,
  udf.mode_last(ARRAY_AGG(cpu_model ORDER BY `timestamp`)) AS cpu_model,
  udf.mode_last(ARRAY_AGG(cpu_speed_mhz ORDER BY `timestamp`)) AS cpu_speed_mhz,
  udf.mode_last(ARRAY_AGG(cpu_stepping ORDER BY `timestamp`)) AS cpu_stepping,
  udf.mode_last(ARRAY_AGG(cpu_vendor ORDER BY `timestamp`)) AS cpu_vendor,
  SUM(crashes_detected_content) AS crashes_detected_content_sum,
  SUM(crashes_detected_gmplugin) AS crashes_detected_gmplugin_sum,
  SUM(crashes_detected_plugin) AS crashes_detected_plugin_sum,
  SUM(crash_submit_attempt_content) AS crash_submit_attempt_content_sum,
  SUM(crash_submit_attempt_main) AS crash_submit_attempt_main_sum,
  SUM(crash_submit_attempt_plugin) AS crash_submit_attempt_plugin_sum,
  SUM(crash_submit_success_content) AS crash_submit_success_content_sum,
  SUM(crash_submit_success_main) AS crash_submit_success_main_sum,
  SUM(crash_submit_success_plugin) AS crash_submit_success_plugin_sum,
  udf.mode_last(ARRAY_AGG(default_search_engine ORDER BY `timestamp`)) AS default_search_engine,
  udf.mode_last(
    ARRAY_AGG(default_search_engine_data_load_path ORDER BY `timestamp`)
  ) AS default_search_engine_data_load_path,
  udf.mode_last(
    ARRAY_AGG(default_search_engine_data_name ORDER BY `timestamp`)
  ) AS default_search_engine_data_name,
  udf.mode_last(
    ARRAY_AGG(default_search_engine_data_origin ORDER BY `timestamp`)
  ) AS default_search_engine_data_origin,
  udf.mode_last(
    ARRAY_AGG(default_search_engine_data_submission_url ORDER BY `timestamp`)
  ) AS default_search_engine_data_submission_url,
  SUM(devtools_toolbox_opened_count) AS devtools_toolbox_opened_count_sum,
  udf.mode_last(ARRAY_AGG(distribution_id ORDER BY `timestamp`)) AS distribution_id,
  udf.mode_last(ARRAY_AGG(e10s_enabled ORDER BY `timestamp`)) AS e10s_enabled,
  udf.mode_last(ARRAY_AGG(env_build_arch ORDER BY `timestamp`)) AS env_build_arch,
  udf.mode_last(ARRAY_AGG(env_build_id ORDER BY `timestamp`)) AS env_build_id,
  udf.mode_last(ARRAY_AGG(env_build_version ORDER BY `timestamp`)) AS env_build_version,
  udf.json_mode_last(
    ARRAY_AGG(
      STRUCT(udf.null_if_empty_list(environment_settings_intl_accept_languages) AS list)
      ORDER BY
        `timestamp`
    )
  ).list AS environment_settings_intl_accept_languages,
  udf.json_mode_last(
    ARRAY_AGG(
      STRUCT(udf.null_if_empty_list(environment_settings_intl_app_locales) AS list)
      ORDER BY
        `timestamp`
    )
  ).list AS environment_settings_intl_app_locales,
  udf.json_mode_last(
    ARRAY_AGG(
      STRUCT(udf.null_if_empty_list(environment_settings_intl_available_locales) AS list)
      ORDER BY
        `timestamp`
    )
  ).list AS environment_settings_intl_available_locales,
  udf.json_mode_last(
    ARRAY_AGG(
      STRUCT(udf.null_if_empty_list(environment_settings_intl_requested_locales) AS list)
      ORDER BY
        `timestamp`
    )
  ).list AS environment_settings_intl_requested_locales,
  udf.json_mode_last(
    ARRAY_AGG(
      STRUCT(udf.null_if_empty_list(environment_settings_intl_system_locales) AS list)
      ORDER BY
        `timestamp`
    )
  ).list AS environment_settings_intl_system_locales,
  udf.json_mode_last(
    ARRAY_AGG(
      STRUCT(udf.null_if_empty_list(environment_settings_intl_regional_prefs_locales) AS list)
      ORDER BY
        `timestamp`
    )
  ).list AS environment_settings_intl_regional_prefs_locales,
  udf.map_mode_last(ARRAY_CONCAT_AGG(experiments ORDER BY `timestamp`)) AS experiments,
  AVG(first_paint) AS first_paint_mean,
  udf.mode_last(ARRAY_AGG(flash_version ORDER BY `timestamp`)) AS flash_version,
  udf.json_mode_last(
    ARRAY_AGG(
      udf.geo_struct(country, city, geo_subdivision1, geo_subdivision2)
      ORDER BY
        `timestamp`
    )
  ).*,
  udf.json_mode_last(ARRAY_AGG(STRUCT(isp_name, isp_organization) ORDER BY `timestamp`)).*,
  udf.mode_last(
    ARRAY_AGG(gfx_features_advanced_layers_status ORDER BY `timestamp`)
  ) AS gfx_features_advanced_layers_status,
  udf.mode_last(ARRAY_AGG(gfx_features_d2d_status ORDER BY `timestamp`)) AS gfx_features_d2d_status,
  udf.mode_last(
    ARRAY_AGG(gfx_features_d3d11_status ORDER BY `timestamp`)
  ) AS gfx_features_d3d11_status,
  udf.mode_last(
    ARRAY_AGG(gfx_features_gpu_process_status ORDER BY `timestamp`)
  ) AS gfx_features_gpu_process_status,
  SUM(
    histogram_parent_devtools_aboutdebugging_opened_count
  ) AS histogram_parent_devtools_aboutdebugging_opened_count_sum,
  SUM(
    histogram_parent_devtools_animationinspector_opened_count
  ) AS histogram_parent_devtools_animationinspector_opened_count_sum,
  SUM(
    histogram_parent_devtools_browserconsole_opened_count
  ) AS histogram_parent_devtools_browserconsole_opened_count_sum,
  SUM(
    histogram_parent_devtools_canvasdebugger_opened_count
  ) AS histogram_parent_devtools_canvasdebugger_opened_count_sum,
  SUM(
    histogram_parent_devtools_computedview_opened_count
  ) AS histogram_parent_devtools_computedview_opened_count_sum,
  SUM(
    histogram_parent_devtools_custom_opened_count
  ) AS histogram_parent_devtools_custom_opened_count_sum,
  NULL AS histogram_parent_devtools_developertoolbar_opened_count_sum, -- deprecated
  SUM(histogram_parent_devtools_dom_opened_count) AS histogram_parent_devtools_dom_opened_count_sum,
  SUM(
    histogram_parent_devtools_eyedropper_opened_count
  ) AS histogram_parent_devtools_eyedropper_opened_count_sum,
  SUM(
    histogram_parent_devtools_fontinspector_opened_count
  ) AS histogram_parent_devtools_fontinspector_opened_count_sum,
  SUM(
    histogram_parent_devtools_inspector_opened_count
  ) AS histogram_parent_devtools_inspector_opened_count_sum,
  SUM(
    histogram_parent_devtools_jsbrowserdebugger_opened_count
  ) AS histogram_parent_devtools_jsbrowserdebugger_opened_count_sum,
  SUM(
    histogram_parent_devtools_jsdebugger_opened_count
  ) AS histogram_parent_devtools_jsdebugger_opened_count_sum,
  SUM(
    histogram_parent_devtools_jsprofiler_opened_count
  ) AS histogram_parent_devtools_jsprofiler_opened_count_sum,
  SUM(
    histogram_parent_devtools_layoutview_opened_count
  ) AS histogram_parent_devtools_layoutview_opened_count_sum,
  SUM(
    histogram_parent_devtools_memory_opened_count
  ) AS histogram_parent_devtools_memory_opened_count_sum,
  SUM(
    histogram_parent_devtools_menu_eyedropper_opened_count
  ) AS histogram_parent_devtools_menu_eyedropper_opened_count_sum,
  SUM(
    histogram_parent_devtools_netmonitor_opened_count
  ) AS histogram_parent_devtools_netmonitor_opened_count_sum,
  SUM(
    histogram_parent_devtools_options_opened_count
  ) AS histogram_parent_devtools_options_opened_count_sum,
  SUM(
    histogram_parent_devtools_paintflashing_opened_count
  ) AS histogram_parent_devtools_paintflashing_opened_count_sum,
  SUM(
    histogram_parent_devtools_picker_eyedropper_opened_count
  ) AS histogram_parent_devtools_picker_eyedropper_opened_count_sum,
  SUM(
    histogram_parent_devtools_responsive_opened_count
  ) AS histogram_parent_devtools_responsive_opened_count_sum,
  SUM(
    histogram_parent_devtools_ruleview_opened_count
  ) AS histogram_parent_devtools_ruleview_opened_count_sum,
  SUM(
    histogram_parent_devtools_scratchpad_opened_count
  ) AS histogram_parent_devtools_scratchpad_opened_count_sum,
  SUM(
    histogram_parent_devtools_scratchpad_window_opened_count
  ) AS histogram_parent_devtools_scratchpad_window_opened_count_sum,
  SUM(
    histogram_parent_devtools_shadereditor_opened_count
  ) AS histogram_parent_devtools_shadereditor_opened_count_sum,
  SUM(
    histogram_parent_devtools_storage_opened_count
  ) AS histogram_parent_devtools_storage_opened_count_sum,
  SUM(
    histogram_parent_devtools_styleeditor_opened_count
  ) AS histogram_parent_devtools_styleeditor_opened_count_sum,
  SUM(
    histogram_parent_devtools_webaudioeditor_opened_count
  ) AS histogram_parent_devtools_webaudioeditor_opened_count_sum,
  SUM(
    histogram_parent_devtools_webconsole_opened_count
  ) AS histogram_parent_devtools_webconsole_opened_count_sum,
  SUM(
    histogram_parent_devtools_webide_opened_count
  ) AS histogram_parent_devtools_webide_opened_count_sum,
  udf.mode_last(ARRAY_AGG(install_year ORDER BY `timestamp`)) AS install_year,
  udf.mode_last(ARRAY_AGG(is_default_browser ORDER BY `timestamp`)) AS is_default_browser,
  udf.mode_last(ARRAY_AGG(is_wow64 ORDER BY `timestamp`)) AS is_wow64,
  udf.mode_last(ARRAY_AGG(locale ORDER BY `timestamp`)) AS locale,
  udf.mode_last(ARRAY_AGG(memory_mb ORDER BY `timestamp`)) AS memory_mb,
  udf.mode_last(ARRAY_AGG(normalized_channel ORDER BY `timestamp`)) AS normalized_channel,
  udf.mode_last(ARRAY_AGG(normalized_os_version ORDER BY `timestamp`)) AS normalized_os_version,
  udf.mode_last(ARRAY_AGG(os ORDER BY `timestamp`)) AS os,
  udf.mode_last(ARRAY_AGG(os_service_pack_major ORDER BY `timestamp`)) AS os_service_pack_major,
  udf.mode_last(ARRAY_AGG(os_service_pack_minor ORDER BY `timestamp`)) AS os_service_pack_minor,
  udf.mode_last(ARRAY_AGG(os_version ORDER BY `timestamp`)) AS os_version,
  COUNT(*) AS pings_aggregated_by_this_row,
  AVG(places_bookmarks_count) AS places_bookmarks_count_mean,
  AVG(places_pages_count) AS places_pages_count_mean,
  SUM(plugin_hangs) AS plugin_hangs_sum,
  SUM(plugins_infobar_allow) AS plugins_infobar_allow_sum,
  SUM(plugins_infobar_block) AS plugins_infobar_block_sum,
  SUM(plugins_infobar_shown) AS plugins_infobar_shown_sum,
  SUM(plugins_notification_shown) AS plugins_notification_shown_sum,
  udf.mode_last(ARRAY_AGG(previous_build_id ORDER BY `timestamp`)) AS previous_build_id,
  UNIX_DATE(DATE(SAFE.TIMESTAMP(ANY_VALUE(subsession_start_date)))) - ANY_VALUE(
    profile_creation_date
  ) AS profile_age_in_days,
  FORMAT_DATE(
    "%F 00:00:00",
    SAFE.DATE_FROM_UNIX_DATE(ANY_VALUE(profile_creation_date))
  ) AS profile_creation_date,
  SUM(push_api_notify) AS push_api_notify_sum,
  ANY_VALUE(sample_id) AS sample_id,
  udf.mode_last(
    ARRAY_AGG(sandbox_effective_content_process_level ORDER BY `timestamp`)
  ) AS sandbox_effective_content_process_level,
  SUM(
    scalar_parent_webrtc_nicer_stun_retransmits + scalar_content_webrtc_nicer_stun_retransmits
  ) AS scalar_combined_webrtc_nicer_stun_retransmits_sum,
  SUM(
    scalar_parent_webrtc_nicer_turn_401s + scalar_content_webrtc_nicer_turn_401s
  ) AS scalar_combined_webrtc_nicer_turn_401s_sum,
  SUM(
    scalar_parent_webrtc_nicer_turn_403s + scalar_content_webrtc_nicer_turn_403s
  ) AS scalar_combined_webrtc_nicer_turn_403s_sum,
  SUM(
    scalar_parent_webrtc_nicer_turn_438s + scalar_content_webrtc_nicer_turn_438s
  ) AS scalar_combined_webrtc_nicer_turn_438s_sum,
  SUM(
    scalar_content_navigator_storage_estimate_count
  ) AS scalar_content_navigator_storage_estimate_count_sum,
  SUM(
    scalar_content_navigator_storage_persist_count
  ) AS scalar_content_navigator_storage_persist_count_sum,
  udf.mode_last(
    ARRAY_AGG(scalar_parent_aushelper_websense_reg_version ORDER BY `timestamp`)
  ) AS scalar_parent_aushelper_websense_reg_version,
  MAX(
    scalar_parent_browser_engagement_max_concurrent_tab_count
  ) AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
  MAX(
    scalar_parent_browser_engagement_max_concurrent_window_count
  ) AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
  SUM(
    scalar_parent_browser_engagement_tab_open_event_count
  ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
  SUM(
    scalar_parent_browser_engagement_total_uri_count
  ) AS scalar_parent_browser_engagement_total_uri_count_sum,
  SUM(
    scalar_parent_browser_engagement_unfiltered_uri_count
  ) AS scalar_parent_browser_engagement_unfiltered_uri_count_sum,
  MAX(
    scalar_parent_browser_engagement_unique_domains_count
  ) AS scalar_parent_browser_engagement_unique_domains_count_max,
  AVG(
    scalar_parent_browser_engagement_unique_domains_count
  ) AS scalar_parent_browser_engagement_unique_domains_count_mean,
  SUM(
    scalar_parent_browser_engagement_window_open_event_count
  ) AS scalar_parent_browser_engagement_window_open_event_count_sum,
  SUM(
    scalar_parent_devtools_accessibility_node_inspected_count
  ) AS scalar_parent_devtools_accessibility_node_inspected_count_sum,
  SUM(
    scalar_parent_devtools_accessibility_opened_count
  ) AS scalar_parent_devtools_accessibility_opened_count_sum,
  SUM(
    scalar_parent_devtools_accessibility_picker_used_count
  ) AS scalar_parent_devtools_accessibility_picker_used_count_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(
      scalar_parent_devtools_accessibility_select_accessible_for_node
      ORDER BY
        `timestamp`
    )
  ) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum,
  SUM(
    scalar_parent_devtools_accessibility_service_enabled_count
  ) AS scalar_parent_devtools_accessibility_service_enabled_count_sum,
  SUM(
    scalar_parent_devtools_copy_full_css_selector_opened
  ) AS scalar_parent_devtools_copy_full_css_selector_opened_sum,
  SUM(
    scalar_parent_devtools_copy_unique_css_selector_opened
  ) AS scalar_parent_devtools_copy_unique_css_selector_opened_sum,
  SUM(
    scalar_parent_devtools_toolbar_eyedropper_opened
  ) AS scalar_parent_devtools_toolbar_eyedropper_opened_sum,
  NULL AS scalar_parent_dom_contentprocess_troubled_due_to_memory_sum, -- deprecated
  SUM(
    scalar_parent_navigator_storage_estimate_count
  ) AS scalar_parent_navigator_storage_estimate_count_sum,
  SUM(
    scalar_parent_navigator_storage_persist_count
  ) AS scalar_parent_navigator_storage_persist_count_sum,
  SUM(
    scalar_parent_storage_sync_api_usage_extensions_using
  ) AS scalar_parent_storage_sync_api_usage_extensions_using_sum,
  udf.mode_last(ARRAY_AGG(search_cohort ORDER BY `timestamp`)) AS search_cohort,
  udf.aggregate_search_counts(ARRAY_CONCAT_AGG(search_counts ORDER BY `timestamp`)).*,
  AVG(session_restored) AS session_restored_mean,
  COUNTIF(subsession_counter = 1) AS sessions_started_on_this_day,
  SUM(shutdown_kill) AS shutdown_kill_sum,
  SUM(subsession_length / NUMERIC '3600') AS subsession_hours_sum,
  SUM(ssl_handshake_result_failure) AS ssl_handshake_result_failure_sum,
  SUM(ssl_handshake_result_success) AS ssl_handshake_result_success_sum,
  udf.mode_last(ARRAY_AGG(sync_configured ORDER BY `timestamp`)) AS sync_configured,
  AVG(sync_count_desktop) AS sync_count_desktop_mean,
  AVG(sync_count_mobile) AS sync_count_mobile_mean,
  SUM(sync_count_desktop) AS sync_count_desktop_sum,
  SUM(sync_count_mobile) AS sync_count_mobile_sum,
  udf.mode_last(ARRAY_AGG(telemetry_enabled ORDER BY `timestamp`)) AS telemetry_enabled,
  udf.mode_last(ARRAY_AGG(timezone_offset ORDER BY `timestamp`)) AS timezone_offset,
  CAST(NULL AS NUMERIC) AS total_hours_sum,
  udf.mode_last(ARRAY_AGG(update_auto_download ORDER BY `timestamp`)) AS update_auto_download,
  udf.mode_last(ARRAY_AGG(update_channel ORDER BY `timestamp`)) AS update_channel,
  udf.mode_last(ARRAY_AGG(update_enabled ORDER BY `timestamp`)) AS update_enabled,
  udf.mode_last(ARRAY_AGG(vendor ORDER BY `timestamp`)) AS vendor,
  SUM(web_notification_shown) AS web_notification_shown_sum,
  udf.mode_last(ARRAY_AGG(windows_build_number ORDER BY `timestamp`)) AS windows_build_number,
  udf.mode_last(ARRAY_AGG(windows_ubr ORDER BY `timestamp`)) AS windows_ubr,
  udf.mode_last(ARRAY_AGG(fxa_configured ORDER BY `timestamp`)) AS fxa_configured,
  SUM(scalar_parent_contentblocking_trackers_blocked_count) AS trackers_blocked_sum,
  TIMESTAMP_MICROS(DIV(MIN(`timestamp`), 1000)) AS submission_timestamp_min,
  SUM(
    (SELECT SUM(value) FROM UNNEST(scalar_parent_browser_search_ad_clicks))
  ) AS ad_clicks_count_all,
  SUM(
    (SELECT SUM(value) FROM UNNEST(scalar_parent_browser_search_with_ads))
  ) AS search_with_ads_count_all,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_telemetry_event_counts)
  ) AS scalar_parent_telemetry_event_counts_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_content_telemetry_event_counts)
  ) AS scalar_content_telemetry_event_counts_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_bookmarkmenu)
  ) AS scalar_parent_urlbar_searchmode_bookmarkmenu_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_handoff)
  ) AS scalar_parent_urlbar_searchmode_handoff_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_keywordoffer)
  ) AS scalar_parent_urlbar_searchmode_keywordoffer_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_oneoff)
  ) AS scalar_parent_urlbar_searchmode_oneoff_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_other)
  ) AS scalar_parent_urlbar_searchmode_other_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_shortcut)
  ) AS scalar_parent_urlbar_searchmode_shortcut_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_tabmenu)
  ) AS scalar_parent_urlbar_searchmode_tabmenu_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_tabtosearch)
  ) AS scalar_parent_urlbar_searchmode_tabtosearch_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_tabtosearch_onboard)
  ) AS scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_topsites_newtab)
  ) AS scalar_parent_urlbar_searchmode_topsites_newtab_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_topsites_urlbar)
  ) AS scalar_parent_urlbar_searchmode_topsites_urlbar_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_touchbar)
  ) AS scalar_parent_urlbar_searchmode_touchbar_sum,
  udf.map_sum(
    ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_typed)
  ) AS scalar_parent_urlbar_searchmode_typed_sum
FROM
  main_summary_v4
LEFT JOIN
  overactive
USING
  (client_id)
WHERE
  submission_date = @submission_date
  -- filter out overactive client_ids to prevent OOM errors
  AND overactive.client_id IS NULL
GROUP BY
  client_id,
  submission_date
