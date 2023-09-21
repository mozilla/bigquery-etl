/*
This query depends on the following fields added to the main_v4 schema on 2019-11-22:

  environment.addons.active_addons[].foreign_install
  environment.addons.active_addons[].user_disabled
  environment.addons.active_addons[].version
  payload.simple_measurements.active_ticks
  payload.simple_measurements.first_paint

To backfill partitions for 2019-11-22 and earlier, use the last version of this query that read from main_summary_v4:
https://github.com/mozilla/bigquery-etl/blob/813a485/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/query.sql
*/
WITH base AS (
  SELECT
    *,
    -- We batch multiple fields into an array here in order to share a single
    -- UDF invocation which keeps query complexity down; order of fields here
    -- is important, as we pull these out by numerical offset later.
    -- count_histograms are deprecated, going forward use scalar.
    ARRAY(
      SELECT AS STRUCT
        mozfun.map.get_key(mozfun.hist.extract(histogram).values, 0) AS histogram
      FROM
        UNNEST(
          [
            payload.histograms.devtools_aboutdebugging_opened_count,
            payload.histograms.devtools_animationinspector_opened_count,
            payload.histograms.devtools_browserconsole_opened_count,
            payload.histograms.devtools_canvasdebugger_opened_count,
            payload.histograms.devtools_computedview_opened_count,
            payload.histograms.devtools_custom_opened_count,
            payload.histograms.devtools_dom_opened_count,
            payload.histograms.devtools_eyedropper_opened_count,
            payload.histograms.devtools_fontinspector_opened_count,
            payload.histograms.devtools_inspector_opened_count,
            payload.histograms.devtools_jsbrowserdebugger_opened_count,
            payload.histograms.devtools_jsdebugger_opened_count,
            payload.histograms.devtools_jsprofiler_opened_count,
            payload.histograms.devtools_layoutview_opened_count,
            payload.histograms.devtools_memory_opened_count,
            payload.histograms.devtools_menu_eyedropper_opened_count,
            payload.histograms.devtools_netmonitor_opened_count,
            payload.histograms.devtools_options_opened_count,
            payload.histograms.devtools_paintflashing_opened_count,
            payload.histograms.devtools_picker_eyedropper_opened_count,
            payload.histograms.devtools_responsive_opened_count,
            payload.histograms.devtools_ruleview_opened_count,
            payload.histograms.devtools_scratchpad_opened_count,
            payload.histograms.devtools_scratchpad_window_opened_count,
            payload.histograms.devtools_shadereditor_opened_count,
            payload.histograms.devtools_storage_opened_count,
            payload.histograms.devtools_styleeditor_opened_count,
            payload.histograms.devtools_webaudioeditor_opened_count,
            payload.histograms.devtools_webconsole_opened_count,
            payload.histograms.devtools_webide_opened_count
          ]
        ) AS histogram
    ) AS count_histograms,
    -- We batch multiple fields into an array here in order to share a single
    -- UDF invocation which keeps query complexity down; order of fields here
    -- is important, as we pull these out by numerical offset later.
    ARRAY(
      SELECT
        udf.extract_histogram_sum(mozfun.map.get_key(histogram, key))
      FROM
        UNNEST(
          [
            STRUCT(
              payload.keyed_histograms.subprocess_crashes_with_dump AS histogram,
              'pluginhang' AS key
            ),
            STRUCT(payload.keyed_histograms.subprocess_abnormal_abort, 'plugin'),
            STRUCT(payload.keyed_histograms.subprocess_abnormal_abort, 'content'),
            STRUCT(payload.keyed_histograms.subprocess_abnormal_abort, 'gmplugin'),
            STRUCT(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin'),
            STRUCT(payload.keyed_histograms.subprocess_crashes_with_dump, 'content'),
            STRUCT(payload.keyed_histograms.subprocess_crashes_with_dump, 'gmplugin'),
            STRUCT(payload.keyed_histograms.process_crash_submit_attempt, 'main-crash'),
            STRUCT(payload.keyed_histograms.process_crash_submit_attempt, 'content-crash'),
            STRUCT(payload.keyed_histograms.process_crash_submit_attempt, 'plugin-crash'),
            STRUCT(payload.keyed_histograms.process_crash_submit_success, 'main-crash'),
            STRUCT(payload.keyed_histograms.process_crash_submit_success, 'content-crash'),
            STRUCT(payload.keyed_histograms.process_crash_submit_success, 'plugin-crash'),
            STRUCT(payload.keyed_histograms.subprocess_kill_hard, 'ShutDownKill'),
            STRUCT(payload.keyed_histograms.fx_migration_bookmarks_quantity, "chrome"),
            STRUCT(payload.keyed_histograms.fx_migration_bookmarks_quantity, "chromium-edge"),
            STRUCT(payload.keyed_histograms.fx_migration_bookmarks_quantity, "safari"),
            STRUCT(payload.keyed_histograms.fx_migration_history_quantity, "chrome"),
            STRUCT(payload.keyed_histograms.fx_migration_history_quantity, "chromium-edge"),
            STRUCT(payload.keyed_histograms.fx_migration_history_quantity, "safari"),
            STRUCT(payload.keyed_histograms.fx_migration_logins_quantity, "chrome"),
            STRUCT(payload.keyed_histograms.fx_migration_logins_quantity, "chromium-edge"),
            STRUCT(payload.keyed_histograms.fx_migration_logins_quantity, "safari"),
            STRUCT(payload.keyed_histograms.media_play_time_ms, 'A'),
            STRUCT(payload.keyed_histograms.media_play_time_ms, 'V')
          ]
        )
    ) AS hist_key_sums,
    -- We batch multiple fields into an array here in order to share a single
    -- UDF invocation which keeps query complexity down; order of fields here
    -- is important, as we pull these out by numerical offset later.
    ARRAY(
      SELECT
        udf.extract_histogram_sum(histogram)
      FROM
        UNNEST(
          [
            payload.histograms.devtools_toolbox_opened_count,
            payload.histograms.push_api_notify,
            payload.histograms.web_notification_shown,
            payload.histograms.plugins_infobar_shown,
            payload.histograms.plugins_infobar_block,
            payload.histograms.plugins_infobar_allow,
            payload.histograms.text_recognition_interaction_timing,
            payload.histograms.text_recognition_api_performance,
            payload.histograms.text_recognition_text_length,
            payload.histograms.places_searchbar_cumulative_searches,
            payload.histograms.places_searchbar_cumulative_filter_count,
            payload.histograms.places_library_cumulative_bookmark_searches,
            payload.histograms.places_library_cumulative_history_searches,
            payload.histograms.places_bookmarks_searchbar_cumulative_searches
          ]
        ) AS histogram
    ) AS hist_sums,
    -- We batch multiple fields into an array here in order to share a single
    -- UDF invocation which keeps query complexity down; order of fields here
    -- is important, as we pull these out by numerical offset later.
    ARRAY(
      SELECT
        mozfun.hist.count(histogram)
      FROM
        UNNEST(
          [
            payload.histograms.text_recognition_interaction_timing,
            payload.histograms.text_recognition_api_performance,
            payload.histograms.text_recognition_text_length
          ]
        ) AS histogram
    ) AS hist_counts,
    -- We batch multiple fields into an array here in order to share a single
    -- UDF invocation which keeps query complexity down; order of fields here
    -- is important, as we pull these out by numerical offset later.
    ARRAY(
      SELECT
        mozfun.hist.extract_keyed_hist_sum(value.keyed_histogram)
      FROM
        UNNEST(
          [
            -- We add a struct layer here b/c BQ doesn't allow nested arrays
            STRUCT(payload.keyed_histograms.fx_migration_bookmarks_quantity AS keyed_histogram),
            STRUCT(payload.keyed_histograms.fx_migration_history_quantity AS keyed_histogram),
            STRUCT(payload.keyed_histograms.fx_migration_logins_quantity AS keyed_histogram)
          ]
        ) AS value
    ) AS keyed_hist_sums,
  FROM
    `moz-fx-data-shared-prod`.telemetry_stable.main_v4
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_app_name = 'Firefox'
    AND document_id IS NOT NULL
),
overactive AS (
  -- Find client_ids with over 150,000 pings in a day, which could errors in the
  -- next step due to aggregation overflows.
  SELECT
    client_id
  FROM
    base
  GROUP BY
    client_id
  HAVING
    COUNT(*) > 150000
),
clients_summary AS (
  SELECT
    submission_timestamp,
    client_id,
    sample_id,
    metadata.uri.app_update_channel AS channel,
    normalized_channel,
    normalized_os_version,
    metadata.geo.country,
    metadata.geo.city,
    metadata.geo.subdivision1 AS geo_subdivision1,
    metadata.geo.subdivision2 AS geo_subdivision2,
    metadata.isp.name AS isp_name,
    metadata.isp.organization AS isp_organization,
    environment.system.os.name AS os,
    environment.system.os.version AS os_version,
    SAFE_CAST(environment.system.os.service_pack_major AS INT64) AS os_service_pack_major,
    SAFE_CAST(environment.system.os.service_pack_minor AS INT64) AS os_service_pack_minor,
    SAFE_CAST(environment.system.os.windows_build_number AS INT64) AS windows_build_number,
    SAFE_CAST(environment.system.os.windows_ubr AS INT64) AS windows_ubr,
    SAFE_CAST(environment.system.os.install_year AS INT64) AS install_year,
    environment.system.is_wow64,
    SAFE_CAST(environment.system.memory_mb AS INT64) AS memory_mb,
    environment.system.cpu.count AS cpu_count,
    environment.system.cpu.cores AS cpu_cores,
    environment.system.cpu.vendor AS cpu_vendor,
    environment.system.cpu.family AS cpu_family,
    environment.system.cpu.model AS cpu_model,
    environment.system.cpu.stepping AS cpu_stepping,
    SAFE_CAST(environment.system.cpu.l2cache_kb AS INT64) AS cpu_l2_cache_kb,
    SAFE_CAST(environment.system.cpu.l3cache_kb AS INT64) AS cpu_l3_cache_kb,
    SAFE_CAST(environment.system.cpu.speed_m_hz AS INT64) AS cpu_speed_mhz,
    environment.system.gfx.features.d3d11.status AS gfx_features_d3d11_status,
    environment.system.gfx.features.d2d.status AS gfx_features_d2d_status,
    environment.system.gfx.features.gpu_process.status AS gfx_features_gpu_process_status,
    environment.system.gfx.features.advanced_layers.status AS gfx_features_advanced_layers_status,
    SAFE_CAST(environment.profile.creation_date AS INT64) AS profile_creation_date,
    payload.info.previous_build_id,
    payload.info.subsession_start_date,
    payload.info.subsession_counter,
    payload.info.subsession_length,
    environment.partner.distribution_id,
    IFNULL(
      environment.services.account_enabled,
      udf.boolean_histogram_to_boolean(payload.histograms.fxa_configured)
    ) AS fxa_configured,
    IFNULL(
      environment.services.sync_enabled,
      udf.boolean_histogram_to_boolean(payload.histograms.weave_configured)
    ) AS sync_configured,
    udf.histogram_max_key_with_nonzero_value(
      payload.histograms.weave_device_count_desktop
    ) AS sync_count_desktop,
    udf.histogram_max_key_with_nonzero_value(
      payload.histograms.weave_device_count_mobile
    ) AS sync_count_mobile,
    application.build_id AS app_build_id,
    application.display_version AS app_display_version,
    application.name AS app_name,
    application.version AS app_version,
    environment.build.build_id AS env_build_id,
    environment.build.version AS env_build_version,
    environment.build.architecture AS env_build_arch,
    environment.settings.e10s_enabled,
    environment.settings.locale,
    environment.settings.update.channel AS update_channel,
    environment.settings.update.enabled AS update_enabled,
    environment.settings.update.auto_download AS update_auto_download,
    environment.settings.update.background AS update_background,
    IF(
      environment.settings.attribution IS NOT NULL,
      STRUCT(
        environment.settings.attribution.source,
        environment.settings.attribution.medium,
        environment.settings.attribution.campaign,
        environment.settings.attribution.content,
        environment.settings.attribution.experiment,
        environment.settings.attribution.variation,
        environment.settings.attribution.dltoken,
        environment.settings.attribution.dlsource
      ),
      NULL
    ) AS attribution,
    environment.settings.sandbox.effective_content_process_level AS sandbox_effective_content_process_level,
    payload.info.timezone_offset,
    -- CAUTION: the order of fields here must match the order defined in
    -- hist_key_sums above and offsets must increment on each line.
    hist_key_sums[OFFSET(0)] AS plugin_hangs,
    hist_key_sums[OFFSET(1)] AS aborts_plugin,
    hist_key_sums[OFFSET(2)] AS aborts_content,
    hist_key_sums[OFFSET(3)] AS aborts_gmplugin,
    hist_key_sums[OFFSET(4)] AS crashes_detected_plugin,
    hist_key_sums[OFFSET(5)] AS crashes_detected_content,
    hist_key_sums[OFFSET(6)] AS crashes_detected_gmplugin,
    hist_key_sums[OFFSET(7)] AS crash_submit_attempt_main,
    hist_key_sums[OFFSET(8)] AS crash_submit_attempt_content,
    hist_key_sums[OFFSET(9)] AS crash_submit_attempt_plugin,
    hist_key_sums[OFFSET(10)] AS crash_submit_success_main,
    hist_key_sums[OFFSET(11)] AS crash_submit_success_content,
    hist_key_sums[OFFSET(12)] AS crash_submit_success_plugin,
    hist_key_sums[OFFSET(13)] AS shutdown_kill,
    hist_key_sums[OFFSET(14)] AS bookmark_migrations_quantity_chrome,
    hist_key_sums[OFFSET(15)] AS bookmark_migrations_quantity_edge,
    hist_key_sums[OFFSET(16)] AS bookmark_migrations_quantity_safari,
    hist_key_sums[OFFSET(17)] AS history_migrations_quantity_chrome,
    hist_key_sums[OFFSET(18)] AS history_migrations_quantity_edge,
    hist_key_sums[OFFSET(19)] AS history_migrations_quantity_safari,
    hist_key_sums[OFFSET(20)] AS logins_migrations_quantity_chrome,
    hist_key_sums[OFFSET(21)] AS logins_migrations_quantity_edge,
    hist_key_sums[OFFSET(22)] AS logins_migrations_quantity_safari,
    hist_key_sums[OFFSET(23)] AS media_play_time_ms_audio,
    hist_key_sums[OFFSET(24)] AS media_play_time_ms_video,
    (
      SELECT
        version
      FROM
        UNNEST(environment.addons.active_plugins),
        UNNEST([STRUCT(SPLIT(version, '.') AS parts)])
      WHERE
        name = 'Shockwave Flash'
      ORDER BY
        SAFE_CAST(parts[SAFE_OFFSET(0)] AS INT64) DESC,
        SAFE_CAST(parts[SAFE_OFFSET(1)] AS INT64) DESC,
        SAFE_CAST(parts[SAFE_OFFSET(2)] AS INT64) DESC,
        SAFE_CAST(parts[SAFE_OFFSET(3)] AS INT64) DESC
      LIMIT
        1
    ) AS flash_version,
    application.vendor,
    environment.settings.is_default_browser,
    environment.settings.default_search_engine_data.name AS default_search_engine_data_name,
    environment.settings.default_search_engine_data.load_path AS default_search_engine_data_load_path,
    environment.settings.default_search_engine_data.origin AS default_search_engine_data_origin,
    environment.settings.default_search_engine_data.submission_url AS default_search_engine_data_submission_url,
    environment.settings.default_search_engine,
    environment.settings.default_private_search_engine_data.name AS default_private_search_engine_data_name,
    environment.settings.default_private_search_engine_data.load_path AS default_private_search_engine_data_load_path,
    environment.settings.default_private_search_engine_data.origin AS default_private_search_engine_data_origin,
    environment.settings.default_private_search_engine_data.submission_url AS default_private_search_engine_data_submission_url,
    environment.settings.default_private_search_engine,
    -- CAUTION: the order of fields here must match the order defined in
    -- hist_sums above and offsets must increment on each line.
    hist_sums[OFFSET(0)] AS devtools_toolbox_opened_count,
    hist_sums[OFFSET(1)] AS push_api_notify,
    hist_sums[OFFSET(2)] AS web_notification_shown,
    hist_sums[OFFSET(3)] AS plugins_infobar_shown,
    hist_sums[OFFSET(4)] AS plugins_infobar_block,
    hist_sums[OFFSET(5)] AS plugins_infobar_allow,
    hist_sums[OFFSET(6)] AS text_recognition_interaction_timing,
    hist_sums[OFFSET(7)] AS text_recognition_api_performance,
    hist_sums[OFFSET(8)] AS text_recognition_text_length,
    hist_sums[OFFSET(9)] AS places_searchbar_cumulative_searches,
    hist_sums[OFFSET(10)] AS places_searchbar_cumulative_filter_count,
    hist_sums[OFFSET(11)] AS places_library_cumulative_bookmark_searches,
    hist_sums[OFFSET(12)] AS places_library_cumulative_history_searches,
    hist_sums[OFFSET(13)] AS places_bookmarks_searchbar_cumulative_searches,
    TIMESTAMP_DIFF(
      TIMESTAMP_TRUNC(submission_timestamp, SECOND),
      SAFE.PARSE_TIMESTAMP('%a, %d %b %Y %T %Z', metadata.header.date),
      SECOND
    ) AS client_clock_skew,
    TIMESTAMP_DIFF(
      TIMESTAMP_TRUNC(submission_timestamp, SECOND),
      SAFE.PARSE_TIMESTAMP('%FT%R:%E*SZ', creation_date),
      SECOND
    ) AS client_submission_latency,
    mozfun.hist.mean(
      mozfun.hist.extract(payload.histograms.places_bookmarks_count)
    ) AS places_bookmarks_count,
    mozfun.hist.mean(
      mozfun.hist.extract(payload.histograms.places_pages_count)
    ) AS places_pages_count,
    ARRAY(
      SELECT AS STRUCT
        SUBSTR(_key, 0, pos - 2) AS engine,
        SUBSTR(_key, pos) AS source,
        udf.extract_histogram_sum(value) AS `count`
      FROM
        UNNEST(payload.keyed_histograms.search_counts),
        -- Bug 1481671 - probe was briefly implemented with '.' rather than ':'
        UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
        UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+?[.].'))]) AS pos
    ) AS search_counts,
    udf_js.main_summary_active_addons(environment.addons.active_addons, NULL) AS active_addons,
    ARRAY_LENGTH(environment.addons.active_addons) AS active_addons_count,
    environment.settings.blocklist_enabled,
    environment.settings.addon_compatibility_check_enabled,
    environment.settings.telemetry_enabled,
    environment.settings.intl.accept_languages AS environment_settings_intl_accept_languages,
    environment.settings.intl.app_locales AS environment_settings_intl_app_locales,
    environment.settings.intl.available_locales AS environment_settings_intl_available_locales,
    environment.settings.intl.regional_prefs_locales AS environment_settings_intl_regional_prefs_locales,
    environment.settings.intl.requested_locales AS environment_settings_intl_requested_locales,
    environment.settings.intl.system_locales AS environment_settings_intl_system_locales,
    SAFE_CAST(
      JSON_EXTRACT_SCALAR(payload.histograms.ssl_handshake_result, '$.values.0') AS INT64
    ) AS ssl_handshake_result_success,
    (
      SELECT
        IFNULL(SUM(value), 0)
      FROM
        UNNEST(mozfun.hist.extract(payload.histograms.ssl_handshake_result).values)
      WHERE
        key
        BETWEEN 1
        AND 671
    ) AS ssl_handshake_result_failure,
    COALESCE(
      payload.processes.parent.scalars.browser_engagement_active_ticks,
      payload.simple_measurements.active_ticks
    ) AS active_ticks,
    COALESCE(
      payload.processes.parent.scalars.timestamps_first_paint,
      payload.simple_measurements.first_paint
    ) AS first_paint,
    payload.simple_measurements.session_restored,
    SAFE_CAST(
      JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_shown, '$.values.1') AS INT64
    ) AS plugins_notification_shown,
    ARRAY(
      SELECT AS STRUCT
        key,
        value.branch AS value
      FROM
        UNNEST(environment.experiments)
    ) AS experiments,
    environment.settings.search_cohort,
    payload.processes.parent.scalars.aushelper_websense_reg_version AS scalar_parent_aushelper_websense_reg_version,
    payload.processes.parent.scalars.browser_engagement_max_concurrent_tab_count AS scalar_parent_browser_engagement_max_concurrent_tab_count,
    payload.processes.parent.scalars.browser_engagement_max_concurrent_window_count AS scalar_parent_browser_engagement_max_concurrent_window_count,
    payload.processes.parent.scalars.browser_engagement_tab_open_event_count AS scalar_parent_browser_engagement_tab_open_event_count,
    payload.processes.parent.scalars.browser_engagement_total_uri_count AS scalar_parent_browser_engagement_total_uri_count,
    payload.processes.parent.scalars.browser_engagement_unfiltered_uri_count AS scalar_parent_browser_engagement_unfiltered_uri_count,
    payload.processes.parent.scalars.browser_engagement_unique_domains_count AS scalar_parent_browser_engagement_unique_domains_count,
    payload.processes.parent.scalars.browser_engagement_window_open_event_count AS scalar_parent_browser_engagement_window_open_event_count,
    payload.processes.parent.scalars.browser_engagement_total_uri_count_normal_and_private_mode AS scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode,
    payload.processes.parent.scalars.contentblocking_trackers_blocked_count AS scalar_parent_contentblocking_trackers_blocked_count,
    payload.processes.parent.scalars.devtools_accessibility_node_inspected_count AS scalar_parent_devtools_accessibility_node_inspected_count,
    payload.processes.parent.scalars.devtools_accessibility_opened_count AS scalar_parent_devtools_accessibility_opened_count,
    payload.processes.parent.scalars.devtools_accessibility_picker_used_count AS scalar_parent_devtools_accessibility_picker_used_count,
    payload.processes.parent.scalars.devtools_accessibility_service_enabled_count AS scalar_parent_devtools_accessibility_service_enabled_count,
    payload.processes.parent.scalars.devtools_copy_full_css_selector_opened AS scalar_parent_devtools_copy_full_css_selector_opened,
    payload.processes.parent.scalars.devtools_copy_unique_css_selector_opened AS scalar_parent_devtools_copy_unique_css_selector_opened,
    payload.processes.parent.scalars.devtools_toolbar_eyedropper_opened AS scalar_parent_devtools_toolbar_eyedropper_opened,
    payload.processes.parent.scalars.navigator_storage_estimate_count AS scalar_parent_navigator_storage_estimate_count,
    payload.processes.parent.scalars.navigator_storage_persist_count AS scalar_parent_navigator_storage_persist_count,
    payload.processes.parent.scalars.os_environment_is_taskbar_pinned AS scalar_parent_os_environment_is_taskbar_pinned,
    payload.processes.parent.scalars.os_environment_launch_method AS scalar_parent_os_environment_launch_method,
    payload.processes.parent.scalars.storage_sync_api_usage_extensions_using AS scalar_parent_storage_sync_api_usage_extensions_using,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_bookmarkmenu AS scalar_parent_urlbar_searchmode_bookmarkmenu,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_handoff AS scalar_parent_urlbar_searchmode_handoff,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_keywordoffer AS scalar_parent_urlbar_searchmode_keywordoffer,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_oneoff AS scalar_parent_urlbar_searchmode_oneoff,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_other AS scalar_parent_urlbar_searchmode_other,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_shortcut AS scalar_parent_urlbar_searchmode_shortcut,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_tabmenu AS scalar_parent_urlbar_searchmode_tabmenu,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_tabtosearch AS scalar_parent_urlbar_searchmode_tabtosearch,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_tabtosearch_onboard AS scalar_parent_urlbar_searchmode_tabtosearch_onboard,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_topsites_newtab AS scalar_parent_urlbar_searchmode_topsites_newtab,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_topsites_urlbar AS scalar_parent_urlbar_searchmode_topsites_urlbar,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_touchbar AS scalar_parent_urlbar_searchmode_touchbar,
    payload.processes.parent.keyed_scalars.urlbar_searchmode_typed AS scalar_parent_urlbar_searchmode_typed,
    payload.processes.parent.keyed_scalars.browser_ui_interaction_preferences_pane_home AS scalar_parent_browser_ui_interaction_preferences_pane_home,
    payload.processes.parent.scalars.urlbar_impression_autofill_about AS scalar_parent_urlbar_impression_autofill_about,
    payload.processes.parent.scalars.urlbar_impression_autofill_adaptive AS scalar_parent_urlbar_impression_autofill_adaptive,
    payload.processes.parent.scalars.urlbar_impression_autofill_origin AS scalar_parent_urlbar_impression_autofill_origin,
    payload.processes.parent.scalars.urlbar_impression_autofill_other AS scalar_parent_urlbar_impression_autofill_other,
    payload.processes.parent.scalars.urlbar_impression_autofill_preloaded AS scalar_parent_urlbar_impression_autofill_preloaded,
    payload.processes.parent.scalars.urlbar_impression_autofill_url AS scalar_parent_urlbar_impression_autofill_url,
    payload.processes.parent.keyed_scalars.urlbar_picked_autofill AS scalar_parent_urlbar_picked_autofill,
    payload.processes.parent.keyed_scalars.urlbar_picked_autofill_about AS scalar_parent_urlbar_picked_autofill_about,
    payload.processes.parent.keyed_scalars.urlbar_picked_autofill_adaptive AS scalar_parent_urlbar_picked_autofill_adaptive,
    payload.processes.parent.keyed_scalars.urlbar_picked_autofill_origin AS scalar_parent_urlbar_picked_autofill_origin,
    payload.processes.parent.keyed_scalars.urlbar_picked_autofill_other AS scalar_parent_urlbar_picked_autofill_other,
    payload.processes.parent.keyed_scalars.urlbar_picked_autofill_preloaded AS scalar_parent_urlbar_picked_autofill_preloaded,
    payload.processes.parent.keyed_scalars.urlbar_picked_autofill_url AS scalar_parent_urlbar_picked_autofill_url,
    payload.processes.parent.keyed_scalars.urlbar_picked_bookmark AS scalar_parent_urlbar_picked_bookmark,
    payload.processes.parent.keyed_scalars.urlbar_picked_dynamic AS scalar_parent_urlbar_picked_dynamic,
    payload.processes.parent.keyed_scalars.urlbar_picked_extension AS scalar_parent_urlbar_picked_extension,
    payload.processes.parent.keyed_scalars.urlbar_picked_formhistory AS scalar_parent_urlbar_picked_formhistory,
    payload.processes.parent.keyed_scalars.urlbar_picked_history AS scalar_parent_urlbar_picked_history,
    payload.processes.parent.keyed_scalars.urlbar_picked_keyword AS scalar_parent_urlbar_picked_keyword,
    payload.processes.parent.keyed_scalars.urlbar_picked_remotetab AS scalar_parent_urlbar_picked_remotetab,
    payload.processes.parent.keyed_scalars.urlbar_picked_searchengine AS scalar_parent_urlbar_picked_searchengine,
    payload.processes.parent.keyed_scalars.urlbar_picked_searchsuggestion AS scalar_parent_urlbar_picked_searchsuggestion,
    payload.processes.parent.keyed_scalars.urlbar_picked_switchtab AS scalar_parent_urlbar_picked_switchtab,
    payload.processes.parent.keyed_scalars.urlbar_picked_tabtosearch AS scalar_parent_urlbar_picked_tabtosearch,
    payload.processes.parent.keyed_scalars.urlbar_picked_tip AS scalar_parent_urlbar_picked_tip,
    payload.processes.parent.keyed_scalars.urlbar_picked_topsite AS scalar_parent_urlbar_picked_topsite,
    payload.processes.parent.keyed_scalars.urlbar_picked_unknown AS scalar_parent_urlbar_picked_unknown,
    payload.processes.parent.keyed_scalars.urlbar_picked_visiturl AS scalar_parent_urlbar_picked_visiturl,
    payload.processes.parent.scalars.webrtc_nicer_stun_retransmits AS scalar_parent_webrtc_nicer_stun_retransmits,
    payload.processes.parent.scalars.webrtc_nicer_turn_401s AS scalar_parent_webrtc_nicer_turn_401s,
    payload.processes.parent.scalars.webrtc_nicer_turn_403s AS scalar_parent_webrtc_nicer_turn_403s,
    payload.processes.parent.scalars.webrtc_nicer_turn_438s AS scalar_parent_webrtc_nicer_turn_438s,
    payload.processes.content.scalars.navigator_storage_estimate_count AS scalar_content_navigator_storage_estimate_count,
    payload.processes.content.scalars.navigator_storage_persist_count AS scalar_content_navigator_storage_persist_count,
    payload.processes.content.scalars.webrtc_nicer_stun_retransmits AS scalar_content_webrtc_nicer_stun_retransmits,
    payload.processes.content.scalars.webrtc_nicer_turn_401s AS scalar_content_webrtc_nicer_turn_401s,
    payload.processes.content.scalars.webrtc_nicer_turn_403s AS scalar_content_webrtc_nicer_turn_403s,
    payload.processes.content.scalars.webrtc_nicer_turn_438s AS scalar_content_webrtc_nicer_turn_438s,
    payload.processes.parent.scalars.a11y_hcm_foreground AS scalar_a11y_hcm_foreground,
    payload.processes.parent.scalars.a11y_hcm_background AS scalar_a11y_hcm_background,
    payload.processes.parent.keyed_scalars.browser_search_ad_clicks AS scalar_parent_browser_search_ad_clicks,
    payload.processes.parent.keyed_scalars.browser_search_with_ads AS scalar_parent_browser_search_with_ads,
    payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node AS scalar_parent_devtools_accessibility_select_accessible_for_node,
    payload.processes.parent.keyed_scalars.telemetry_event_counts AS scalar_parent_telemetry_event_counts,
    payload.processes.content.keyed_scalars.telemetry_event_counts AS scalar_content_telemetry_event_counts,
    payload.processes.parent.keyed_scalars.browser_search_content_urlbar,
    payload.processes.parent.keyed_scalars.browser_search_content_urlbar_searchmode,
    payload.processes.parent.keyed_scalars.browser_search_content_contextmenu,
    payload.processes.parent.keyed_scalars.browser_search_content_about_home,
    payload.processes.parent.keyed_scalars.browser_search_content_about_newtab,
    payload.processes.parent.keyed_scalars.browser_search_content_searchbar,
    payload.processes.parent.keyed_scalars.browser_search_content_system,
    payload.processes.parent.keyed_scalars.browser_search_content_webextension,
    payload.processes.parent.keyed_scalars.browser_search_content_tabhistory,
    payload.processes.parent.keyed_scalars.browser_search_content_reload,
    payload.processes.parent.keyed_scalars.browser_search_content_unknown,
    payload.processes.parent.keyed_scalars.browser_search_withads_urlbar,
    payload.processes.parent.keyed_scalars.browser_search_withads_urlbar_searchmode,
    payload.processes.parent.keyed_scalars.browser_search_withads_contextmenu,
    payload.processes.parent.keyed_scalars.browser_search_withads_about_home,
    payload.processes.parent.keyed_scalars.browser_search_withads_about_newtab,
    payload.processes.parent.keyed_scalars.browser_search_withads_searchbar,
    payload.processes.parent.keyed_scalars.browser_search_withads_system,
    payload.processes.parent.keyed_scalars.browser_search_withads_webextension,
    payload.processes.parent.keyed_scalars.browser_search_withads_tabhistory,
    payload.processes.parent.keyed_scalars.browser_search_withads_reload,
    payload.processes.parent.keyed_scalars.browser_search_withads_unknown,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_urlbar,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_urlbar_searchmode,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_contextmenu,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_about_home,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_about_newtab,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_searchbar,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_system,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_webextension,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_tabhistory,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_reload,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_unknown,
    payload.processes.parent.keyed_scalars.browser_search_content_urlbar_handoff,
    payload.processes.parent.keyed_scalars.browser_search_withads_urlbar_handoff,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_urlbar_handoff,
    payload.processes.parent.keyed_scalars.browser_search_content_urlbar_persisted,
    payload.processes.parent.keyed_scalars.browser_search_withads_urlbar_persisted,
    payload.processes.parent.keyed_scalars.browser_search_adclicks_urlbar_persisted,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_block_dynamic_wikipedia,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_block_nonsponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_block_nonsponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_block_sponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_block_sponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_block_weather,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_click,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_click_dynamic_wikipedia,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_click_nonsponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_click_nonsponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_click_sponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_click_sponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_click_weather,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_help,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_help_dynamic_wikipedia,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_help_nonsponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_help_nonsponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_help_sponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_help_sponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_help_weather,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_impression,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_impression_dynamic_wikipedia,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_impression_nonsponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_impression_nonsponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_impression_sponsored,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_impression_sponsored_bestmatch,
    payload.processes.parent.keyed_scalars.contextual_services_quicksuggest_impression_weather,
    payload.processes.parent.keyed_scalars.contextual_services_topsites_click,
    payload.processes.parent.keyed_scalars.contextual_services_topsites_impression,
    payload.processes.parent.keyed_scalars.a11y_theme,
    payload.processes.parent.keyed_scalars.browser_ui_interaction_content_context AS scalar_parent_browser_ui_interaction_content_context,
    payload.processes.parent.scalars.browser_ui_interaction_textrecognition_error AS scalar_parent_browser_ui_interaction_textrecognition_error,
    payload.processes.parent.keyed_scalars.sidebar_opened AS scalar_parent_sidebar_opened,
    payload.processes.parent.keyed_scalars.sidebar_search AS scalar_parent_sidebar_search,
    payload.processes.parent.keyed_scalars.sidebar_link AS scalar_parent_sidebar_link,
    payload.processes.parent.keyed_scalars.library_link AS scalar_parent_library_link,
    payload.processes.parent.keyed_scalars.library_opened AS scalar_parent_library_opened,
    payload.processes.parent.keyed_scalars.library_search AS scalar_parent_library_search,
    payload.processes.parent.scalars.places_previousday_visits AS places_previousday_visits,
    -- CAUTION: the order of fields here must match the order defined in
    -- count_histograms above and offsets must increment on each line.
    count_histograms[OFFSET(0)].histogram AS histogram_parent_devtools_aboutdebugging_opened_count,
    count_histograms[
      OFFSET(1)
    ].histogram AS histogram_parent_devtools_animationinspector_opened_count,
    count_histograms[OFFSET(2)].histogram AS histogram_parent_devtools_browserconsole_opened_count,
    count_histograms[OFFSET(3)].histogram AS histogram_parent_devtools_canvasdebugger_opened_count,
    count_histograms[OFFSET(4)].histogram AS histogram_parent_devtools_computedview_opened_count,
    count_histograms[OFFSET(5)].histogram AS histogram_parent_devtools_custom_opened_count,
    count_histograms[OFFSET(6)].histogram AS histogram_parent_devtools_dom_opened_count,
    count_histograms[OFFSET(7)].histogram AS histogram_parent_devtools_eyedropper_opened_count,
    count_histograms[OFFSET(8)].histogram AS histogram_parent_devtools_fontinspector_opened_count,
    count_histograms[OFFSET(9)].histogram AS histogram_parent_devtools_inspector_opened_count,
    count_histograms[
      OFFSET(10)
    ].histogram AS histogram_parent_devtools_jsbrowserdebugger_opened_count,
    count_histograms[OFFSET(11)].histogram AS histogram_parent_devtools_jsdebugger_opened_count,
    count_histograms[OFFSET(12)].histogram AS histogram_parent_devtools_jsprofiler_opened_count,
    count_histograms[OFFSET(13)].histogram AS histogram_parent_devtools_layoutview_opened_count,
    count_histograms[OFFSET(14)].histogram AS histogram_parent_devtools_memory_opened_count,
    count_histograms[
      OFFSET(15)
    ].histogram AS histogram_parent_devtools_menu_eyedropper_opened_count,
    count_histograms[OFFSET(16)].histogram AS histogram_parent_devtools_netmonitor_opened_count,
    count_histograms[OFFSET(17)].histogram AS histogram_parent_devtools_options_opened_count,
    count_histograms[OFFSET(18)].histogram AS histogram_parent_devtools_paintflashing_opened_count,
    count_histograms[
      OFFSET(19)
    ].histogram AS histogram_parent_devtools_picker_eyedropper_opened_count,
    count_histograms[OFFSET(20)].histogram AS histogram_parent_devtools_responsive_opened_count,
    count_histograms[OFFSET(21)].histogram AS histogram_parent_devtools_ruleview_opened_count,
    count_histograms[OFFSET(22)].histogram AS histogram_parent_devtools_scratchpad_opened_count,
    count_histograms[
      OFFSET(23)
    ].histogram AS histogram_parent_devtools_scratchpad_window_opened_count,
    count_histograms[OFFSET(24)].histogram AS histogram_parent_devtools_shadereditor_opened_count,
    count_histograms[OFFSET(25)].histogram AS histogram_parent_devtools_storage_opened_count,
    count_histograms[OFFSET(26)].histogram AS histogram_parent_devtools_styleeditor_opened_count,
    count_histograms[OFFSET(27)].histogram AS histogram_parent_devtools_webaudioeditor_opened_count,
    count_histograms[OFFSET(28)].histogram AS histogram_parent_devtools_webconsole_opened_count,
    count_histograms[OFFSET(29)].histogram AS histogram_parent_devtools_webide_opened_count,
    -- CAUTION: the order of fields here must match the order defined in
    -- hist_counts above and offsets must increment on each line.
    hist_counts[OFFSET(0)] AS text_recognition_interaction_timing_count,
    hist_counts[OFFSET(1)] AS text_recognition_api_performance_count,
    hist_counts[OFFSET(2)] AS text_recognition_text_length_count,
    -- CAUTION: the order of fields here must match the order defined in
    -- keyed_hist_sums above and offsets must increment on each line.
    keyed_hist_sums[OFFSET(0)] AS bookmark_migrations_quantity_all,
    keyed_hist_sums[OFFSET(1)] AS history_migrations_quantity_all,
    keyed_hist_sums[OFFSET(2)] AS logins_migrations_quantity_all,
    COALESCE(
      payload.processes.parent.scalars.dom_parentprocess_private_window_used,
      FALSE
    ) AS scalar_parent_dom_parentprocess_private_window_used,
    COALESCE(
      payload.processes.parent.scalars.os_environment_is_taskbar_pinned,
      FALSE
    ) AS scalar_parent_os_environment_is_taskbar_pinned_any,
    COALESCE(
      payload.processes.parent.scalars.os_environment_is_taskbar_pinned_private,
      FALSE
    ) AS scalar_parent_os_environment_is_taskbar_pinned_private,
    -- Select out some individual userPrefs values; note that prefs are only available in
    -- the environment based on registration in DEFAULT_ENVIRONMENT_PREFS; see
    -- https://searchfox.org/mozilla-central/source/toolkit/components/telemetry/app/TelemetryEnvironment.jsm
    (
      SELECT AS STRUCT
        ARRAY_AGG(IF(key = 'browser.search.region', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_search_region,
        ARRAY_AGG(IF(key = 'browser.search.suggest.enabled', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_search_suggest_enabled,
        ARRAY_AGG(IF(key = 'browser.search.widget.inNavBar', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_widget_in_navbar,
        ARRAY_AGG(IF(key = 'browser.urlbar.suggest.searches', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_urlbar_suggest_searches,
        ARRAY_AGG(IF(key = 'browser.urlbar.showSearchSuggestionsFirst', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_urlbar_show_search_suggestions_first,
        ARRAY_AGG(IF(key = 'browser.urlbar.suggest.quicksuggest', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_urlbar_suggest_quicksuggest,
        -- Rename of browser.urlbar.suggest.quicksuggest.nonsponsored; see bug 1737374
        ARRAY_AGG(
          IF(key = 'browser.urlbar.suggest.quicksuggest.nonsponsored', value, NULL) IGNORE NULLS
        )[SAFE_OFFSET(0)] AS user_pref_browser_urlbar_suggest_quicksuggest_nonsponsored,
        ARRAY_AGG(
          IF(key = 'browser.urlbar.suggest.quicksuggest.sponsored', value, NULL) IGNORE NULLS
        )[SAFE_OFFSET(0)] AS user_pref_browser_urlbar_suggest_quicksuggest_sponsored,
        ARRAY_AGG(
          IF(key = 'browser.urlbar.quicksuggest.onboardingDialogChoice', value, NULL) IGNORE NULLS
        )[SAFE_OFFSET(0)] AS user_pref_browser_urlbar_quicksuggest_onboarding_dialog_choice,
        -- New pref for Firefox Suggest introduced in bug 1737374
        ARRAY_AGG(
          IF(key = 'browser.urlbar.quicksuggest.dataCollection.enabled', value, NULL) IGNORE NULLS
        )[SAFE_OFFSET(0)] AS user_pref_browser_urlbar_quicksuggest_data_collection_enabled,
        -- New pref for Firefox Suggest introduced in bug 1756917
        ARRAY_AGG(IF(key = 'browser.urlbar.suggest.bestmatch', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_urlbar_suggest_bestmatch,
        ARRAY_AGG(IF(key = 'browser.newtabpage.enabled', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_browser_newtabpage_enabled,
        ARRAY_AGG(IF(key = 'app.shield.optoutstudies.enabled', value, NULL) IGNORE NULLS)[
          SAFE_OFFSET(0)
        ] AS user_pref_app_shield_optoutstudies_enabled,
      FROM
        UNNEST(environment.settings.user_prefs)
    ).*
  FROM
    base
  LEFT JOIN
    overactive
    USING (client_id)
  WHERE
    overactive.client_id IS NULL
),
aggregates AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    SUM(aborts_content) AS aborts_content_sum,
    SUM(aborts_gmplugin) AS aborts_gmplugin_sum,
    SUM(aborts_plugin) AS aborts_plugin_sum,
    AVG(active_addons_count) AS active_addons_count_mean,
    udf.aggregate_active_addons(
      ARRAY_CONCAT_AGG(active_addons ORDER BY submission_timestamp)
    ) AS active_addons,
    CAST(
      NULL AS STRING
    ) AS active_experiment_branch, -- deprecated
    CAST(
      NULL AS STRING
    ) AS active_experiment_id, -- deprecated
    SUM(active_ticks / (3600 / 5)) AS active_hours_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(addon_compatibility_check_enabled ORDER BY submission_timestamp)
    ) AS addon_compatibility_check_enabled,
    mozfun.stats.mode_last(ARRAY_AGG(app_build_id ORDER BY submission_timestamp)) AS app_build_id,
    mozfun.stats.mode_last(
      ARRAY_AGG(app_display_version ORDER BY submission_timestamp)
    ) AS app_display_version,
    mozfun.stats.mode_last(ARRAY_AGG(app_name ORDER BY submission_timestamp)) AS app_name,
    mozfun.stats.mode_last(ARRAY_AGG(app_version ORDER BY submission_timestamp)) AS app_version,
    mozfun.json.mode_last(ARRAY_AGG(attribution ORDER BY submission_timestamp)) AS attribution,
    mozfun.stats.mode_last(
      ARRAY_AGG(blocklist_enabled ORDER BY submission_timestamp)
    ) AS blocklist_enabled,
    mozfun.stats.mode_last(ARRAY_AGG(channel ORDER BY submission_timestamp)) AS channel,
    AVG(client_clock_skew) AS client_clock_skew_mean,
    AVG(client_submission_latency) AS client_submission_latency_mean,
    mozfun.stats.mode_last(ARRAY_AGG(cpu_cores ORDER BY submission_timestamp)) AS cpu_cores,
    mozfun.stats.mode_last(ARRAY_AGG(cpu_count ORDER BY submission_timestamp)) AS cpu_count,
    mozfun.stats.mode_last(ARRAY_AGG(cpu_family ORDER BY submission_timestamp)) AS cpu_family,
    mozfun.stats.mode_last(
      ARRAY_AGG(cpu_l2_cache_kb ORDER BY submission_timestamp)
    ) AS cpu_l2_cache_kb,
    mozfun.stats.mode_last(
      ARRAY_AGG(cpu_l3_cache_kb ORDER BY submission_timestamp)
    ) AS cpu_l3_cache_kb,
    mozfun.stats.mode_last(ARRAY_AGG(cpu_model ORDER BY submission_timestamp)) AS cpu_model,
    mozfun.stats.mode_last(ARRAY_AGG(cpu_speed_mhz ORDER BY submission_timestamp)) AS cpu_speed_mhz,
    mozfun.stats.mode_last(ARRAY_AGG(cpu_stepping ORDER BY submission_timestamp)) AS cpu_stepping,
    mozfun.stats.mode_last(ARRAY_AGG(cpu_vendor ORDER BY submission_timestamp)) AS cpu_vendor,
    SUM(crashes_detected_content) AS crashes_detected_content_sum,
    SUM(crashes_detected_gmplugin) AS crashes_detected_gmplugin_sum,
    SUM(crashes_detected_plugin) AS crashes_detected_plugin_sum,
    SUM(crash_submit_attempt_content) AS crash_submit_attempt_content_sum,
    SUM(crash_submit_attempt_main) AS crash_submit_attempt_main_sum,
    SUM(crash_submit_attempt_plugin) AS crash_submit_attempt_plugin_sum,
    SUM(crash_submit_success_content) AS crash_submit_success_content_sum,
    SUM(crash_submit_success_main) AS crash_submit_success_main_sum,
    SUM(crash_submit_success_plugin) AS crash_submit_success_plugin_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_search_engine ORDER BY submission_timestamp)
    ) AS default_search_engine,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_search_engine_data_load_path ORDER BY submission_timestamp)
    ) AS default_search_engine_data_load_path,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_search_engine_data_name ORDER BY submission_timestamp)
    ) AS default_search_engine_data_name,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_search_engine_data_origin ORDER BY submission_timestamp)
    ) AS default_search_engine_data_origin,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_search_engine_data_submission_url ORDER BY submission_timestamp)
    ) AS default_search_engine_data_submission_url,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_private_search_engine ORDER BY submission_timestamp)
    ) AS default_private_search_engine,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_private_search_engine_data_load_path ORDER BY submission_timestamp)
    ) AS default_private_search_engine_data_load_path,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_private_search_engine_data_name ORDER BY submission_timestamp)
    ) AS default_private_search_engine_data_name,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_private_search_engine_data_origin ORDER BY submission_timestamp)
    ) AS default_private_search_engine_data_origin,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_private_search_engine_data_submission_url ORDER BY submission_timestamp)
    ) AS default_private_search_engine_data_submission_url,
    SUM(devtools_toolbox_opened_count) AS devtools_toolbox_opened_count_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(distribution_id ORDER BY submission_timestamp)
    ) AS distribution_id,
    mozfun.stats.mode_last(ARRAY_AGG(e10s_enabled ORDER BY submission_timestamp)) AS e10s_enabled,
    mozfun.stats.mode_last(
      ARRAY_AGG(env_build_arch ORDER BY submission_timestamp)
    ) AS env_build_arch,
    mozfun.stats.mode_last(ARRAY_AGG(env_build_id ORDER BY submission_timestamp)) AS env_build_id,
    mozfun.stats.mode_last(
      ARRAY_AGG(env_build_version ORDER BY submission_timestamp)
    ) AS env_build_version,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(environment_settings_intl_accept_languages) > 0,
          STRUCT(environment_settings_intl_accept_languages AS list),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ).list AS environment_settings_intl_accept_languages,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(environment_settings_intl_app_locales) > 0,
          STRUCT(environment_settings_intl_app_locales AS list),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ).list AS environment_settings_intl_app_locales,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(environment_settings_intl_available_locales) > 0,
          STRUCT(environment_settings_intl_available_locales AS list),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ).list AS environment_settings_intl_available_locales,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(environment_settings_intl_requested_locales) > 0,
          STRUCT(environment_settings_intl_requested_locales AS list),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ).list AS environment_settings_intl_requested_locales,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(environment_settings_intl_system_locales) > 0,
          STRUCT(environment_settings_intl_system_locales AS list),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ).list AS environment_settings_intl_system_locales,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          ARRAY_LENGTH(environment_settings_intl_regional_prefs_locales) > 0,
          STRUCT(environment_settings_intl_regional_prefs_locales AS list),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ).list AS environment_settings_intl_regional_prefs_locales,
    mozfun.map.mode_last(
      ARRAY_CONCAT_AGG(experiments ORDER BY submission_timestamp)
    ) AS experiments,
    AVG(first_paint) AS first_paint_mean,
    mozfun.stats.mode_last(ARRAY_AGG(flash_version ORDER BY submission_timestamp)) AS flash_version,
    mozfun.json.mode_last(
      ARRAY_AGG(
        udf.geo_struct(country, city, geo_subdivision1, geo_subdivision2)
        ORDER BY
          submission_timestamp
      )
    ).*,
    mozfun.json.mode_last(
      ARRAY_AGG(
        IF(
          isp_name IS NOT NULL
          OR isp_organization IS NOT NULL,
          STRUCT(isp_name, isp_organization),
          NULL
        )
        ORDER BY
          submission_timestamp
      )
    ).*,
    mozfun.stats.mode_last(
      ARRAY_AGG(gfx_features_advanced_layers_status ORDER BY submission_timestamp)
    ) AS gfx_features_advanced_layers_status,
    mozfun.stats.mode_last(
      ARRAY_AGG(gfx_features_d2d_status ORDER BY submission_timestamp)
    ) AS gfx_features_d2d_status,
    mozfun.stats.mode_last(
      ARRAY_AGG(gfx_features_d3d11_status ORDER BY submission_timestamp)
    ) AS gfx_features_d3d11_status,
    mozfun.stats.mode_last(
      ARRAY_AGG(gfx_features_gpu_process_status ORDER BY submission_timestamp)
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
    SUM(
      histogram_parent_devtools_dom_opened_count
    ) AS histogram_parent_devtools_dom_opened_count_sum,
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
    mozfun.stats.mode_last(ARRAY_AGG(install_year ORDER BY submission_timestamp)) AS install_year,
    mozfun.stats.mode_last(
      ARRAY_AGG(is_default_browser ORDER BY submission_timestamp)
    ) AS is_default_browser,
    mozfun.stats.mode_last(ARRAY_AGG(is_wow64 ORDER BY submission_timestamp)) AS is_wow64,
    mozfun.stats.mode_last(ARRAY_AGG(locale ORDER BY submission_timestamp)) AS locale,
    mozfun.stats.mode_last(ARRAY_AGG(memory_mb ORDER BY submission_timestamp)) AS memory_mb,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_channel ORDER BY submission_timestamp)
    ) AS normalized_channel,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_os_version ORDER BY submission_timestamp)
    ) AS normalized_os_version,
    mozfun.stats.mode_last(ARRAY_AGG(os ORDER BY submission_timestamp)) AS os,
    mozfun.stats.mode_last(
      ARRAY_AGG(os_service_pack_major ORDER BY submission_timestamp)
    ) AS os_service_pack_major,
    mozfun.stats.mode_last(
      ARRAY_AGG(os_service_pack_minor ORDER BY submission_timestamp)
    ) AS os_service_pack_minor,
    mozfun.stats.mode_last(ARRAY_AGG(os_version ORDER BY submission_timestamp)) AS os_version,
    COUNT(*) AS pings_aggregated_by_this_row,
    AVG(places_bookmarks_count) AS places_bookmarks_count_mean,
    AVG(places_pages_count) AS places_pages_count_mean,
    SUM(plugin_hangs) AS plugin_hangs_sum,
    SUM(plugins_infobar_allow) AS plugins_infobar_allow_sum,
    SUM(plugins_infobar_block) AS plugins_infobar_block_sum,
    SUM(plugins_infobar_shown) AS plugins_infobar_shown_sum,
    SUM(plugins_notification_shown) AS plugins_notification_shown_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(previous_build_id ORDER BY submission_timestamp)
    ) AS previous_build_id,
    MAX(UNIX_DATE(DATE(SAFE.TIMESTAMP(subsession_start_date)))) - MAX(
      profile_creation_date
    ) AS profile_age_in_days,
    FORMAT_DATE(
      "%F 00:00:00",
      SAFE.DATE_FROM_UNIX_DATE(MAX(profile_creation_date))
    ) AS profile_creation_date,
    SUM(push_api_notify) AS push_api_notify_sum,
    ANY_VALUE(sample_id) AS sample_id,
    mozfun.stats.mode_last(
      ARRAY_AGG(sandbox_effective_content_process_level ORDER BY submission_timestamp)
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
    mozfun.stats.mode_last(
      ARRAY_AGG(scalar_parent_aushelper_websense_reg_version ORDER BY submission_timestamp)
    ) AS scalar_parent_aushelper_websense_reg_version,
    mozfun.stats.mode_last(
      ARRAY_AGG(scalar_a11y_hcm_foreground ORDER BY submission_timestamp)
    ) AS scalar_a11y_hcm_foreground,
    mozfun.stats.mode_last(
      ARRAY_AGG(scalar_a11y_hcm_background ORDER BY submission_timestamp)
    ) AS scalar_a11y_hcm_background,
    mozfun.map.mode_last(ARRAY_CONCAT_AGG(a11y_theme ORDER BY submission_timestamp)) AS a11y_theme,
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
      scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode
    ) AS scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum,
    SUM(
      scalar_parent_devtools_accessibility_node_inspected_count
    ) AS scalar_parent_devtools_accessibility_node_inspected_count_sum,
    SUM(
      scalar_parent_devtools_accessibility_opened_count
    ) AS scalar_parent_devtools_accessibility_opened_count_sum,
    SUM(
      scalar_parent_devtools_accessibility_picker_used_count
    ) AS scalar_parent_devtools_accessibility_picker_used_count_sum,
    mozfun.map.sum(
      ARRAY_CONCAT_AGG(
        scalar_parent_devtools_accessibility_select_accessible_for_node
        ORDER BY
          submission_timestamp
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
    mozfun.stats.mode_last(
      ARRAY_AGG(scalar_parent_os_environment_is_taskbar_pinned ORDER BY submission_timestamp)
    ) AS scalar_parent_os_environment_is_taskbar_pinned,
    COUNTIF(
      'Desktop' = scalar_parent_os_environment_launch_method
    ) > 0 AS scalar_parent_os_environment_launched_via_desktop,
    COUNTIF(
      'StartMenu' = scalar_parent_os_environment_launch_method
    ) > 0 AS scalar_parent_os_environment_launched_via_start_menu,
    COUNTIF(
      'Taskbar' = scalar_parent_os_environment_launch_method
    ) > 0 AS scalar_parent_os_environment_launched_via_taskbar,
    COUNTIF(
      'OtherShortcut' = scalar_parent_os_environment_launch_method
    ) > 0 AS scalar_parent_os_environment_launched_via_other_shortcut,
    COUNTIF(
      'Other' = scalar_parent_os_environment_launch_method
    ) > 0 AS scalar_parent_os_environment_launched_via_other,
    COUNTIF(
      'TaskbarPrivate' = scalar_parent_os_environment_launch_method
    ) > 0 AS scalar_parent_os_environment_launched_via_taskbar_private,
    SUM(
      scalar_parent_storage_sync_api_usage_extensions_using
    ) AS scalar_parent_storage_sync_api_usage_extensions_using_sum,
    mozfun.stats.mode_last(ARRAY_AGG(search_cohort ORDER BY submission_timestamp)) AS search_cohort,
    udf.aggregate_search_counts(ARRAY_CONCAT_AGG(search_counts ORDER BY submission_timestamp)).*,
    AVG(session_restored) AS session_restored_mean,
    COUNTIF(subsession_counter = 1) AS sessions_started_on_this_day,
    SUM(shutdown_kill) AS shutdown_kill_sum,
    SUM(subsession_length / NUMERIC '3600') AS subsession_hours_sum,
    SUM(ssl_handshake_result_failure) AS ssl_handshake_result_failure_sum,
    SUM(ssl_handshake_result_success) AS ssl_handshake_result_success_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(sync_configured ORDER BY submission_timestamp)
    ) AS sync_configured,
    AVG(sync_count_desktop) AS sync_count_desktop_mean,
    AVG(sync_count_mobile) AS sync_count_mobile_mean,
    SUM(sync_count_desktop) AS sync_count_desktop_sum,
    SUM(sync_count_mobile) AS sync_count_mobile_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(telemetry_enabled ORDER BY submission_timestamp)
    ) AS telemetry_enabled,
    mozfun.stats.mode_last(
      ARRAY_AGG(timezone_offset ORDER BY submission_timestamp)
    ) AS timezone_offset,
    CAST(NULL AS NUMERIC) AS total_hours_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(update_auto_download ORDER BY submission_timestamp)
    ) AS update_auto_download,
    mozfun.stats.mode_last(
      ARRAY_AGG(update_channel ORDER BY submission_timestamp)
    ) AS update_channel,
    mozfun.stats.mode_last(
      ARRAY_AGG(update_enabled ORDER BY submission_timestamp)
    ) AS update_enabled,
    mozfun.stats.mode_last(
      ARRAY_AGG(update_background ORDER BY submission_timestamp)
    ) AS update_background,
    mozfun.stats.mode_last(ARRAY_AGG(vendor ORDER BY submission_timestamp)) AS vendor,
    SUM(web_notification_shown) AS web_notification_shown_sum,
    mozfun.stats.mode_last(
      ARRAY_AGG(windows_build_number ORDER BY submission_timestamp)
    ) AS windows_build_number,
    mozfun.stats.mode_last(ARRAY_AGG(windows_ubr ORDER BY submission_timestamp)) AS windows_ubr,
    mozfun.stats.mode_last(
      ARRAY_AGG(fxa_configured ORDER BY submission_timestamp)
    ) AS fxa_configured,
    SUM(scalar_parent_contentblocking_trackers_blocked_count) AS trackers_blocked_sum,
    MIN(submission_timestamp) AS submission_timestamp_min,
    -- prioritize access point based probes
    COALESCE(
      SUM(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              ARRAY_CONCAT(
                browser_search_adclicks_urlbar,
                browser_search_adclicks_urlbar_searchmode,
                browser_search_adclicks_contextmenu,
                browser_search_adclicks_about_home,
                browser_search_adclicks_about_newtab,
                browser_search_adclicks_searchbar,
                browser_search_adclicks_system,
                browser_search_adclicks_webextension,
                browser_search_adclicks_tabhistory,
                browser_search_adclicks_reload,
                browser_search_adclicks_unknown,
                browser_search_adclicks_urlbar_handoff,
                browser_search_adclicks_urlbar_persisted
              )
            )
        )
      ),
      SUM((SELECT SUM(value) FROM UNNEST(scalar_parent_browser_search_ad_clicks)))
    ) AS ad_clicks_count_all,
    COALESCE(
      SUM(
        (
          SELECT
            SUM(value)
          FROM
            UNNEST(
              ARRAY_CONCAT(
                browser_search_withads_urlbar,
                browser_search_withads_urlbar_searchmode,
                browser_search_withads_contextmenu,
                browser_search_withads_about_home,
                browser_search_withads_about_newtab,
                browser_search_withads_searchbar,
                browser_search_withads_system,
                browser_search_withads_webextension,
                browser_search_withads_tabhistory,
                browser_search_withads_reload,
                browser_search_withads_unknown,
                browser_search_withads_urlbar_handoff,
                browser_search_withads_urlbar_persisted
              )
            )
        )
      ),
      SUM((SELECT SUM(value) FROM UNNEST(scalar_parent_browser_search_with_ads)))
    ) AS search_with_ads_count_all,
    SUM(
      scalar_parent_urlbar_impression_autofill_about
    ) AS scalar_parent_urlbar_impression_autofill_about_sum,
    SUM(
      scalar_parent_urlbar_impression_autofill_adaptive
    ) AS scalar_parent_urlbar_impression_autofill_adaptive_sum,
    SUM(
      scalar_parent_urlbar_impression_autofill_origin
    ) AS scalar_parent_urlbar_impression_autofill_origin_sum,
    SUM(
      scalar_parent_urlbar_impression_autofill_other
    ) AS scalar_parent_urlbar_impression_autofill_other_sum,
    SUM(
      scalar_parent_urlbar_impression_autofill_preloaded
    ) AS scalar_parent_urlbar_impression_autofill_preloaded_sum,
    SUM(
      scalar_parent_urlbar_impression_autofill_url
    ) AS scalar_parent_urlbar_impression_autofill_url_sum,
    AVG(places_previousday_visits) AS places_previousday_visits_mean,
    -- We batch multiple fields into an array here in order to share a single
    -- UDF invocation in the udf_aggregates CTE below which keeps query
    -- complexity down; order of fields here is important, as we pull these out
    -- by numerical offset later.
    [
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_telemetry_event_counts) AS agg),
      STRUCT(ARRAY_CONCAT_AGG(scalar_content_telemetry_event_counts)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_bookmarkmenu)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_keywordoffer)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_oneoff)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_other)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_shortcut)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_tabmenu)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_tabtosearch)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_tabtosearch_onboard)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_topsites_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_topsites_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_touchbar)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_searchmode_typed)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_browser_ui_interaction_preferences_pane_home)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_autofill)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_autofill_about)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_autofill_adaptive)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_autofill_origin)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_autofill_other)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_autofill_preloaded)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_autofill_url)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_bookmark)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_dynamic)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_extension)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_formhistory)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_history)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_keyword)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_remotetab)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_searchengine)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_searchsuggestion)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_switchtab)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_tabtosearch)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_tip)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_topsite)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_unknown)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_urlbar_picked_visiturl)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_browser_search_with_ads)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_browser_search_ad_clicks)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_urlbar_searchmode)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_contextmenu)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_about_home)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_about_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_searchbar)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_system)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_webextension)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_tabhistory)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_reload)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_unknown)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_urlbar_searchmode)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_contextmenu)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_about_home)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_about_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_searchbar)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_system)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_webextension)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_tabhistory)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_reload)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_unknown)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_urlbar)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_urlbar_searchmode)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_contextmenu)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_about_home)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_about_newtab)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_searchbar)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_system)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_webextension)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_tabhistory)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_reload)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_unknown)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_urlbar_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_urlbar_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_urlbar_handoff)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_block_dynamic_wikipedia)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_block_nonsponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_block_nonsponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_block_sponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_block_sponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_block_weather)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_click)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_click_dynamic_wikipedia)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_click_nonsponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_click_nonsponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_click_sponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_click_sponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_click_weather)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_help)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_help_dynamic_wikipedia)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_help_nonsponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_help_nonsponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_help_sponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_help_sponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_help_weather)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_impression)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_impression_dynamic_wikipedia)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_impression_nonsponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_impression_nonsponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_impression_sponsored)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_impression_sponsored_bestmatch)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_quicksuggest_impression_weather)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_topsites_click)),
      STRUCT(ARRAY_CONCAT_AGG(contextual_services_topsites_impression)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_browser_ui_interaction_content_context)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_content_urlbar_persisted)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_withads_urlbar_persisted)),
      STRUCT(ARRAY_CONCAT_AGG(browser_search_adclicks_urlbar_persisted)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_sidebar_opened)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_sidebar_search)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_sidebar_link)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_library_link)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_library_opened)),
      STRUCT(ARRAY_CONCAT_AGG(scalar_parent_library_search))
    ] AS map_sum_aggregates,
    udf.search_counts_map_sum(ARRAY_CONCAT_AGG(search_counts)) AS search_counts,
    mozfun.stats.mode_last(
      ARRAY_AGG(user_pref_browser_search_region ORDER BY submission_timestamp)
    ) AS user_pref_browser_search_region,
    mozfun.stats.mode_last(
      ARRAY_AGG(user_pref_browser_search_suggest_enabled ORDER BY submission_timestamp)
    ) AS user_pref_browser_search_suggest_enabled,
    mozfun.stats.mode_last(
      ARRAY_AGG(user_pref_browser_widget_in_navbar ORDER BY submission_timestamp)
    ) AS user_pref_browser_widget_in_navbar,
    mozfun.stats.mode_last(
      ARRAY_AGG(user_pref_browser_urlbar_suggest_searches ORDER BY submission_timestamp)
    ) AS user_pref_browser_urlbar_suggest_searches,
    mozfun.stats.mode_last(
      ARRAY_AGG(
        user_pref_browser_urlbar_show_search_suggestions_first
        ORDER BY
          submission_timestamp
      )
    ) AS user_pref_browser_urlbar_show_search_suggestions_first,
    mozfun.stats.mode_last(
      ARRAY_AGG(user_pref_browser_newtabpage_enabled ORDER BY submission_timestamp)
    ) AS user_pref_browser_newtabpage_enabled,
    mozfun.stats.mode_last(
      ARRAY_AGG(user_pref_app_shield_optoutstudies_enabled ORDER BY submission_timestamp)
    ) AS user_pref_app_shield_optoutstudies_enabled,
    -- We use last seen value rather than mode_last for all Firefox Suggest-related
    -- pref values to ensure all values represent the same ping.
    ARRAY_AGG(user_pref_browser_urlbar_suggest_quicksuggest ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS user_pref_browser_urlbar_suggest_quicksuggest,
    ARRAY_AGG(
      user_pref_browser_urlbar_suggest_quicksuggest_nonsponsored
      ORDER BY
        submission_timestamp DESC
    )[OFFSET(0)] AS user_pref_browser_urlbar_suggest_quicksuggest_nonsponsored,
    ARRAY_AGG(
      user_pref_browser_urlbar_suggest_quicksuggest_sponsored
      ORDER BY
        submission_timestamp DESC
    )[OFFSET(0)] AS user_pref_browser_urlbar_suggest_quicksuggest_sponsored,
    ARRAY_AGG(
      user_pref_browser_urlbar_quicksuggest_onboarding_dialog_choice
      ORDER BY
        submission_timestamp DESC
    )[OFFSET(0)] AS user_pref_browser_urlbar_quicksuggest_onboarding_dialog_choice,
    ARRAY_AGG(
      user_pref_browser_urlbar_quicksuggest_data_collection_enabled
      ORDER BY
        submission_timestamp DESC
    )[OFFSET(0)] AS user_pref_browser_urlbar_quicksuggest_data_collection_enabled,
    ARRAY_AGG(user_pref_browser_urlbar_suggest_bestmatch ORDER BY submission_timestamp DESC)[
      OFFSET(0)
    ] AS user_pref_browser_urlbar_suggest_bestmatch,
    SUM(
      scalar_parent_browser_ui_interaction_textrecognition_error
    ) AS scalar_parent_browser_ui_interaction_textrecognition_error_sum,
    SUM(text_recognition_interaction_timing) AS text_recognition_interaction_timing_sum,
    SUM(text_recognition_interaction_timing_count) AS text_recognition_interaction_timing_count_sum,
    SUM(text_recognition_api_performance) AS text_recognition_api_performance_sum,
    SUM(text_recognition_api_performance_count) AS text_recognition_api_performance_count_sum,
    SUM(text_recognition_text_length) AS text_recognition_text_length_sum,
    SUM(text_recognition_text_length_count) AS text_recognition_text_length_count_sum,
    SUM(places_searchbar_cumulative_searches) AS places_searchbar_cumulative_searches_sum,
    SUM(places_searchbar_cumulative_filter_count) AS places_searchbar_cumulative_filter_count_sum,
    SUM(
      places_library_cumulative_bookmark_searches
    ) AS places_library_cumulative_bookmark_searches_sum,
    SUM(
      places_library_cumulative_history_searches
    ) AS places_library_cumulative_history_searches_sum,
    SUM(
      places_bookmarks_searchbar_cumulative_searches
    ) AS places_bookmarks_searchbar_cumulative_searches_sum,
    LOGICAL_OR(
      scalar_parent_dom_parentprocess_private_window_used
    ) AS dom_parentprocess_private_window_used,
    LOGICAL_OR(
      scalar_parent_os_environment_is_taskbar_pinned_any
    ) AS os_environment_is_taskbar_pinned_any,
    LOGICAL_OR(
      scalar_parent_os_environment_is_taskbar_pinned_private
    ) AS os_environment_is_taskbar_pinned_private_any,
    mozfun.stats.mode_last(
      ARRAY_AGG(
        scalar_parent_os_environment_is_taskbar_pinned_private
        ORDER BY
          submission_timestamp
      )
    ) AS os_environment_is_taskbar_pinned_private,
    SUM(bookmark_migrations_quantity_chrome) AS bookmark_migrations_quantity_chrome,
    SUM(bookmark_migrations_quantity_edge) AS bookmark_migrations_quantity_edge,
    SUM(bookmark_migrations_quantity_safari) AS bookmark_migrations_quantity_safari,
    SUM(bookmark_migrations_quantity_all) AS bookmark_migrations_quantity_all,
    SUM(history_migrations_quantity_chrome) AS history_migrations_quantity_chrome,
    SUM(history_migrations_quantity_edge) AS history_migrations_quantity_edge,
    SUM(history_migrations_quantity_safari) AS history_migrations_quantity_safari,
    SUM(history_migrations_quantity_all) AS history_migrations_quantity_all,
    SUM(logins_migrations_quantity_chrome) AS logins_migrations_quantity_chrome,
    SUM(logins_migrations_quantity_edge) AS logins_migrations_quantity_edge,
    SUM(logins_migrations_quantity_safari) AS logins_migrations_quantity_safari,
    SUM(logins_migrations_quantity_all) AS logins_migrations_quantity_all,
    SUM(media_play_time_ms_audio) AS media_play_time_ms_audio_sum,
    SUM(media_play_time_ms_video) AS media_play_time_ms_video_sum,
  FROM
    clients_summary
  GROUP BY
    client_id,
    submission_date
),
udf_aggregates AS (
  SELECT
    * REPLACE (
      ARRAY(
        SELECT AS STRUCT
          mozfun.map.sum(agg) AS map,
        FROM
          UNNEST(map_sum_aggregates)
      ) AS map_sum_aggregates
    )
  FROM
    aggregates
)
SELECT
  * EXCEPT (map_sum_aggregates),
  -- CAUTION: the order of fields here must match the order defined in
  -- map_sum_aggregates above and offsets must increment on each line.
  map_sum_aggregates[OFFSET(0)].map AS scalar_parent_telemetry_event_counts_sum,
  map_sum_aggregates[OFFSET(1)].map AS scalar_content_telemetry_event_counts_sum,
  map_sum_aggregates[OFFSET(2)].map AS scalar_parent_urlbar_searchmode_bookmarkmenu_sum,
  map_sum_aggregates[OFFSET(3)].map AS scalar_parent_urlbar_searchmode_handoff_sum,
  map_sum_aggregates[OFFSET(4)].map AS scalar_parent_urlbar_searchmode_keywordoffer_sum,
  map_sum_aggregates[OFFSET(5)].map AS scalar_parent_urlbar_searchmode_oneoff_sum,
  map_sum_aggregates[OFFSET(6)].map AS scalar_parent_urlbar_searchmode_other_sum,
  map_sum_aggregates[OFFSET(7)].map AS scalar_parent_urlbar_searchmode_shortcut_sum,
  map_sum_aggregates[OFFSET(8)].map AS scalar_parent_urlbar_searchmode_tabmenu_sum,
  map_sum_aggregates[OFFSET(9)].map AS scalar_parent_urlbar_searchmode_tabtosearch_sum,
  map_sum_aggregates[OFFSET(10)].map AS scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum,
  map_sum_aggregates[OFFSET(11)].map AS scalar_parent_urlbar_searchmode_topsites_newtab_sum,
  map_sum_aggregates[OFFSET(12)].map AS scalar_parent_urlbar_searchmode_topsites_urlbar_sum,
  map_sum_aggregates[OFFSET(13)].map AS scalar_parent_urlbar_searchmode_touchbar_sum,
  map_sum_aggregates[OFFSET(14)].map AS scalar_parent_urlbar_searchmode_typed_sum,
  map_sum_aggregates[
    OFFSET(15)
  ].map AS scalar_parent_browser_ui_interaction_preferences_pane_home_sum,
  map_sum_aggregates[OFFSET(16)].map AS scalar_parent_urlbar_picked_autofill_sum,
  map_sum_aggregates[OFFSET(17)].map AS scalar_parent_urlbar_picked_autofill_about_sum,
  map_sum_aggregates[OFFSET(18)].map AS scalar_parent_urlbar_picked_autofill_adaptive_sum,
  map_sum_aggregates[OFFSET(19)].map AS scalar_parent_urlbar_picked_autofill_origin_sum,
  map_sum_aggregates[OFFSET(20)].map AS scalar_parent_urlbar_picked_autofill_other_sum,
  map_sum_aggregates[OFFSET(21)].map AS scalar_parent_urlbar_picked_autofill_preloaded_sum,
  map_sum_aggregates[OFFSET(22)].map AS scalar_parent_urlbar_picked_autofill_url_sum,
  map_sum_aggregates[OFFSET(23)].map AS scalar_parent_urlbar_picked_bookmark_sum,
  map_sum_aggregates[OFFSET(24)].map AS scalar_parent_urlbar_picked_dynamic_sum,
  map_sum_aggregates[OFFSET(25)].map AS scalar_parent_urlbar_picked_extension_sum,
  map_sum_aggregates[OFFSET(26)].map AS scalar_parent_urlbar_picked_formhistory_sum,
  map_sum_aggregates[OFFSET(27)].map AS scalar_parent_urlbar_picked_history_sum,
  map_sum_aggregates[OFFSET(28)].map AS scalar_parent_urlbar_picked_keyword_sum,
  map_sum_aggregates[OFFSET(29)].map AS scalar_parent_urlbar_picked_remotetab_sum,
  map_sum_aggregates[OFFSET(30)].map AS scalar_parent_urlbar_picked_searchengine_sum,
  map_sum_aggregates[OFFSET(31)].map AS scalar_parent_urlbar_picked_searchsuggestion_sum,
  map_sum_aggregates[OFFSET(32)].map AS scalar_parent_urlbar_picked_switchtab_sum,
  map_sum_aggregates[OFFSET(33)].map AS scalar_parent_urlbar_picked_tabtosearch_sum,
  map_sum_aggregates[OFFSET(34)].map AS scalar_parent_urlbar_picked_tip_sum,
  map_sum_aggregates[OFFSET(35)].map AS scalar_parent_urlbar_picked_topsite_sum,
  map_sum_aggregates[OFFSET(36)].map AS scalar_parent_urlbar_picked_unknown_sum,
  map_sum_aggregates[OFFSET(37)].map AS scalar_parent_urlbar_picked_visiturl_sum,
  map_sum_aggregates[OFFSET(38)].map AS search_with_ads,
  map_sum_aggregates[OFFSET(39)].map AS ad_clicks,
  map_sum_aggregates[OFFSET(40)].map AS search_content_urlbar_sum,
  map_sum_aggregates[OFFSET(41)].map AS search_content_urlbar_searchmode_sum,
  map_sum_aggregates[OFFSET(42)].map AS search_content_contextmenu_sum,
  map_sum_aggregates[OFFSET(43)].map AS search_content_about_home_sum,
  map_sum_aggregates[OFFSET(44)].map AS search_content_about_newtab_sum,
  map_sum_aggregates[OFFSET(45)].map AS search_content_searchbar_sum,
  map_sum_aggregates[OFFSET(46)].map AS search_content_system_sum,
  map_sum_aggregates[OFFSET(47)].map AS search_content_webextension_sum,
  map_sum_aggregates[OFFSET(48)].map AS search_content_tabhistory_sum,
  map_sum_aggregates[OFFSET(49)].map AS search_content_reload_sum,
  map_sum_aggregates[OFFSET(50)].map AS search_content_unknown_sum,
  map_sum_aggregates[OFFSET(51)].map AS search_withads_urlbar_sum,
  map_sum_aggregates[OFFSET(52)].map AS search_withads_urlbar_searchmode_sum,
  map_sum_aggregates[OFFSET(53)].map AS search_withads_contextmenu_sum,
  map_sum_aggregates[OFFSET(54)].map AS search_withads_about_home_sum,
  map_sum_aggregates[OFFSET(55)].map AS search_withads_about_newtab_sum,
  map_sum_aggregates[OFFSET(56)].map AS search_withads_searchbar_sum,
  map_sum_aggregates[OFFSET(57)].map AS search_withads_system_sum,
  map_sum_aggregates[OFFSET(58)].map AS search_withads_webextension_sum,
  map_sum_aggregates[OFFSET(59)].map AS search_withads_tabhistory_sum,
  map_sum_aggregates[OFFSET(60)].map AS search_withads_reload_sum,
  map_sum_aggregates[OFFSET(61)].map AS search_withads_unknown_sum,
  map_sum_aggregates[OFFSET(62)].map AS search_adclicks_urlbar_sum,
  map_sum_aggregates[OFFSET(63)].map AS search_adclicks_urlbar_searchmode_sum,
  map_sum_aggregates[OFFSET(64)].map AS search_adclicks_contextmenu_sum,
  map_sum_aggregates[OFFSET(65)].map AS search_adclicks_about_home_sum,
  map_sum_aggregates[OFFSET(66)].map AS search_adclicks_about_newtab_sum,
  map_sum_aggregates[OFFSET(67)].map AS search_adclicks_searchbar_sum,
  map_sum_aggregates[OFFSET(68)].map AS search_adclicks_system_sum,
  map_sum_aggregates[OFFSET(69)].map AS search_adclicks_webextension_sum,
  map_sum_aggregates[OFFSET(70)].map AS search_adclicks_tabhistory_sum,
  map_sum_aggregates[OFFSET(71)].map AS search_adclicks_reload_sum,
  map_sum_aggregates[OFFSET(72)].map AS search_adclicks_unknown_sum,
  map_sum_aggregates[OFFSET(73)].map AS search_content_urlbar_handoff_sum,
  map_sum_aggregates[OFFSET(74)].map AS search_withads_urlbar_handoff_sum,
  map_sum_aggregates[OFFSET(75)].map AS search_adclicks_urlbar_handoff_sum,
  map_sum_aggregates[
    OFFSET(76)
  ].map AS contextual_services_quicksuggest_block_dynamic_wikipedia_sum,
  map_sum_aggregates[OFFSET(77)].map AS contextual_services_quicksuggest_block_nonsponsored_sum,
  map_sum_aggregates[
    OFFSET(78)
  ].map AS contextual_services_quicksuggest_block_nonsponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(79)].map AS contextual_services_quicksuggest_block_sponsored_sum,
  map_sum_aggregates[
    OFFSET(80)
  ].map AS contextual_services_quicksuggest_block_sponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(81)].map AS contextual_services_quicksuggest_block_weather_sum,
  map_sum_aggregates[OFFSET(82)].map AS contextual_services_quicksuggest_click_sum,
  map_sum_aggregates[
    OFFSET(83)
  ].map AS contextual_services_quicksuggest_click_dynamic_wikipedia_sum,
  map_sum_aggregates[OFFSET(84)].map AS contextual_services_quicksuggest_click_nonsponsored_sum,
  map_sum_aggregates[
    OFFSET(85)
  ].map AS contextual_services_quicksuggest_click_nonsponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(86)].map AS contextual_services_quicksuggest_click_sponsored_sum,
  map_sum_aggregates[
    OFFSET(87)
  ].map AS contextual_services_quicksuggest_click_sponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(88)].map AS contextual_services_quicksuggest_click_weather_sum,
  map_sum_aggregates[OFFSET(89)].map AS contextual_services_quicksuggest_help_sum,
  map_sum_aggregates[OFFSET(90)].map AS contextual_services_quicksuggest_help_dynamic_wikipedia_sum,
  map_sum_aggregates[OFFSET(91)].map AS contextual_services_quicksuggest_help_nonsponsored_sum,
  map_sum_aggregates[
    OFFSET(92)
  ].map AS contextual_services_quicksuggest_help_nonsponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(93)].map AS contextual_services_quicksuggest_help_sponsored_sum,
  map_sum_aggregates[
    OFFSET(94)
  ].map AS contextual_services_quicksuggest_help_sponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(95)].map AS contextual_services_quicksuggest_help_weather_sum,
  map_sum_aggregates[OFFSET(96)].map AS contextual_services_quicksuggest_impression_sum,
  map_sum_aggregates[
    OFFSET(97)
  ].map AS contextual_services_quicksuggest_impression_dynamic_wikipedia_sum,
  map_sum_aggregates[
    OFFSET(98)
  ].map AS contextual_services_quicksuggest_impression_nonsponsored_sum,
  map_sum_aggregates[
    OFFSET(99)
  ].map AS contextual_services_quicksuggest_impression_nonsponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(100)].map AS contextual_services_quicksuggest_impression_sponsored_sum,
  map_sum_aggregates[
    OFFSET(101)
  ].map AS contextual_services_quicksuggest_impression_sponsored_bestmatch_sum,
  map_sum_aggregates[OFFSET(102)].map AS contextual_services_quicksuggest_impression_weather_sum,
  map_sum_aggregates[OFFSET(103)].map AS contextual_services_topsites_click_sum,
  map_sum_aggregates[OFFSET(104)].map AS contextual_services_topsites_impression_sum,
  map_sum_aggregates[OFFSET(105)].map AS scalar_parent_browser_ui_interaction_content_context_sum,
  map_sum_aggregates[OFFSET(106)].map AS search_content_urlbar_persisted_sum,
  map_sum_aggregates[OFFSET(107)].map AS search_withads_urlbar_persisted_sum,
  map_sum_aggregates[OFFSET(108)].map AS search_adclicks_urlbar_persisted_sum,
  map_sum_aggregates[OFFSET(109)].map AS scalar_parent_sidebar_opened_sum,
  map_sum_aggregates[OFFSET(110)].map AS scalar_parent_sidebar_search_sum,
  map_sum_aggregates[OFFSET(111)].map AS scalar_parent_sidebar_link_sum,
  map_sum_aggregates[OFFSET(112)].map AS scalar_parent_library_link_sum,
  map_sum_aggregates[OFFSET(113)].map AS scalar_parent_library_opened_sum,
  map_sum_aggregates[OFFSET(114)].map AS scalar_parent_library_search_sum,
FROM
  udf_aggregates
