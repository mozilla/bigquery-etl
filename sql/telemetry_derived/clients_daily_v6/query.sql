CREATE TEMP FUNCTION
  udf_json_mode_last(list ANY TYPE) AS ((
    SELECT
      ANY_VALUE(_value)
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS _offset
    GROUP BY
      TO_JSON_STRING(_value)
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1));
CREATE TEMP FUNCTION udf_aggregate_active_addons(active_addons ANY TYPE) AS (
  ARRAY(
    SELECT
      udf_json_mode_last(ARRAY_AGG(element))
    FROM
      UNNEST(active_addons) AS element
    GROUP BY
      element.addon_id
  )
);
CREATE TEMP FUNCTION udf_aggregate_search_counts(
  search_counts ARRAY<
    STRUCT<
      engine STRING,
      source STRING,
      count INT64
    >
  >
) AS (
  (
    SELECT AS STRUCT
      SUM(count) AS search_count_all,
      SUM(IF(
        source = "abouthome",
        count,
        0
      )) AS search_count_abouthome,
      SUM(IF(
        source = "contextmenu",
        count,
        0
      )) AS search_count_contextmenu,
      SUM(IF(
        source = "newtab",
        count,
        0
      )) AS search_count_newtab,
      SUM(IF(
        source = "searchbar",
        count,
        0
      )) AS search_count_searchbar,
      SUM(IF(
        source = "system",
        count,
        0
      )) AS search_count_system,
      SUM(IF(
        source = "urlbar",
        count,
        0
      )) AS search_count_urlbar
    FROM
      UNNEST(search_counts)
    WHERE
      source IN (
        "abouthome",
        "contextmenu",
        "newtab",
        "searchbar",
        "system",
        "urlbar"
      )
  )
);
CREATE TEMP FUNCTION
  udf_boolean_histogram_to_boolean(histogram STRING) AS (
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(histogram,
          "$.values.1") AS INT64) > 0,
      NOT SAFE_CAST( JSON_EXTRACT_SCALAR(histogram,
          "$.values.0") AS INT64) > 0));
CREATE TEMP FUNCTION
  udf_geo_struct(country STRING,
    city STRING,
    geo_subdivision1 STRING,
    geo_subdivision2 STRING) AS ( IF(country IS NULL
      OR country = '??',
      NULL,
      STRUCT(country,
        NULLIF(city,
          '??') AS city,
        NULLIF(geo_subdivision1,
          '??') AS geo_subdivision1,
        NULLIF(geo_subdivision2,
          '??') AS geo_subdivision2)));
CREATE TEMP FUNCTION udf_get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);
CREATE TEMP FUNCTION udf_histogram_to_mean(histogram ANY TYPE) AS (
  CASE
  WHEN histogram.sum < 0 THEN NULL
  WHEN histogram.sum = 0 THEN 0
  ELSE SAFE_CAST(TRUNC(histogram.sum / (SELECT SUM(value) FROM UNNEST(histogram.values) WHERE value > 0)) AS INT64)
  END
);
CREATE TEMP FUNCTION
  udf_json_extract_int_map (input STRING) AS (ARRAY(
    SELECT
      STRUCT(CAST(SPLIT(entry, ':')[OFFSET(0)] AS INT64) AS key,
             CAST(SPLIT(entry, ':')[OFFSET(1)] AS INT64) AS value)
    FROM
      UNNEST(SPLIT(REPLACE(TRIM(input, '{}'), '"', ''), ',')) AS entry
    WHERE
      LENGTH(entry) > 0 ));
CREATE TEMP FUNCTION
  udf_json_extract_histogram (input STRING) AS (STRUCT(
    CAST(JSON_EXTRACT_SCALAR(input, '$.bucket_count') AS INT64) AS bucket_count,
    CAST(JSON_EXTRACT_SCALAR(input, '$.histogram_type') AS INT64) AS histogram_type,
    CAST(JSON_EXTRACT_SCALAR(input, '$.sum') AS INT64) AS `sum`,
    ARRAY(
      SELECT
        CAST(bound AS INT64)
      FROM
        UNNEST(SPLIT(TRIM(JSON_EXTRACT(input, '$.range'), '[]'), ',')) AS bound) AS `range`,
    udf_json_extract_int_map(JSON_EXTRACT(input, '$.values')) AS `values` ));
CREATE TEMP FUNCTION udf_main_summary_active_addons(active_addons ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key AS addon_id,
      value.blocklisted,
      value.name,
      value.user_disabled > 0 AS user_disabled,
      value.app_disabled,
      value.version,
      value.scope,
      value.type,
      value.foreign_install > 0 AS foreign_install,
      value.has_binary_components,
      value.install_day,
      value.update_day,
      value.signed_state,
      value.is_system,
      value.is_web_extension,
      value.multiprocess_compatible
    FROM
      UNNEST(active_addons)
  )
);
CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));
CREATE TEMP FUNCTION udf_map_mode_last(entries ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key,
      udf_mode_last(ARRAY_AGG(value)) AS value
    FROM
      UNNEST(entries)
    GROUP BY
      key
  )
);
CREATE TEMP FUNCTION udf_map_sum(entries ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key,
      SUM(value) AS value
    FROM
      UNNEST(entries)
    GROUP BY
      key
  )
);
CREATE TEMP FUNCTION udf_max_flash_version(active_plugins ANY TYPE) AS (
  (
    SELECT
      version
    FROM
      UNNEST(active_plugins),
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
  )
);
CREATE TEMP FUNCTION udf_null_if_empty_list(list ANY TYPE) AS (
  IF(
    ARRAY_LENGTH(list) > 0,
    list,
    NULL
  )
);
CREATE TEMP FUNCTION udf_search_counts(search_counts ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      SUBSTR(_key, 0, pos - 2) AS engine,
      SUBSTR(_key, pos) AS source,
      udf_json_extract_histogram(value).sum AS `count`
    FROM
      UNNEST(search_counts),
      UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
      UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
  )
);
--
WITH overactive AS (
  -- find client_ids with over 200,000 pings in a day
  SELECT
    client_id
  FROM
    telemetry.main
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
  HAVING
    COUNT(*) > 200000
), pings AS (
  SELECT
    *,
    (
      SELECT AS STRUCT
        ANY_VALUE(IF(key = "content", `sum`, NULL)) AS content,
        ANY_VALUE(IF(key = "gmplugin", `sum`, NULL)) AS gmplugin,
        ANY_VALUE(IF(key = "plugin", `sum`, NULL)) AS plugin
      FROM
        UNNEST(payload.keyed_histograms.subprocess_abnormal_abort),
        UNNEST([udf_json_extract_histogram(value).sum]) AS `sum`
    ) AS _aborts,
    udf_main_summary_active_addons(environment.addons.active_addons) AS _active_addons,
    TIMESTAMP_DIFF(SAFE.PARSE_TIMESTAMP("%a, %d %b %Y %T %Z", metadata.header.date), submission_timestamp, SECOND) AS _client_clock_skew,
    TIMESTAMP_DIFF(SAFE.PARSE_TIMESTAMP("%FT%R:%E*SZ", creation_date), submission_timestamp, SECOND) AS _client_submission_latency,
    (
      SELECT AS STRUCT
        ANY_VALUE(IF(key = "content", `sum`, NULL)) AS content,
        ANY_VALUE(IF(key = "gmplugin", `sum`, NULL)) AS gmplugin,
        ANY_VALUE(IF(key = "plugin", `sum`, NULL)) AS plugin
      FROM
        UNNEST(payload.keyed_histograms.subprocess_crashes_with_dump),
        UNNEST([udf_json_extract_histogram(value).sum]) AS `sum`
    ) AS _crashes_detected,
    (
      SELECT AS STRUCT
        ANY_VALUE(IF(key = "main_crash", `sum`, NULL)) AS main,
        ANY_VALUE(IF(key = "content_crash", `sum`, NULL)) AS content,
        ANY_VALUE(IF(key = "plugin_crash", `sum`, NULL)) AS plugin
      FROM
        UNNEST(payload.keyed_histograms.process_crash_submit_attempt),
        UNNEST([udf_json_extract_histogram(value).sum]) AS `sum`
    ) AS _crash_submit_attempt,
    (
      SELECT AS STRUCT
        ANY_VALUE(IF(key = "main_crash", `sum`, NULL)) AS main,
        ANY_VALUE(IF(key = "content_crash", `sum`, NULL)) AS content,
        ANY_VALUE(IF(key = "plugin_crash", `sum`, NULL)) AS plugin
      FROM
        UNNEST(payload.keyed_histograms.process_crash_submit_success),
        UNNEST([udf_json_extract_histogram(value).sum]) AS `sum`
    ) AS _crash_submit_success,
    ARRAY(
      SELECT AS STRUCT
        key,
        value.branch AS value
      FROM
        UNNEST(environment.experiments)
    ) AS _experiments,
    IFNULL(
      payload.processes.parent.scalars.timestamps_first_paint,
      payload.simple_measurements.first_paint
    ) AS _first_paint,
    udf_max_flash_version(environment.addons.active_plugins) AS _flash_version,
    SAFE_CAST(JSON_EXTRACT(payload.histograms.weave_device_count_desktop, '$.values.0') AS INT64) AS _sync_count_desktop,
    SAFE_CAST(JSON_EXTRACT(payload.histograms.weave_device_count_mobile, '$.values.0') AS INT64) AS _sync_count_mobile
  FROM
    telemetry.main
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_id,
  SUM(_aborts.content) AS aborts_content_sum,
  SUM(_aborts.gmplugin) AS aborts_gmplugin_sum,
  SUM(_aborts.plugin) AS aborts_plugin_sum,
  AVG(ARRAY_LENGTH(_active_addons)) AS active_addons_count_mean,
  udf_aggregate_active_addons(ARRAY_CONCAT_AGG(_active_addons ORDER BY submission_timestamp)) AS active_addons,
  CAST(NULL AS STRING) AS active_experiment_branch, -- deprecated
  CAST(NULL AS STRING) AS active_experiment_id, -- deprecated
  SUM(payload.simple_measurements.active_ticks/(3600/5)) AS active_hours_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.addon_compatibility_check_enabled ORDER BY submission_timestamp)) AS addon_compatibility_check_enabled,
  udf_mode_last(ARRAY_AGG(application.build_id ORDER BY submission_timestamp)) AS app_build_id,
  udf_mode_last(ARRAY_AGG(application.display_version ORDER BY submission_timestamp)) AS app_display_version,
  udf_mode_last(ARRAY_AGG(application.name ORDER BY submission_timestamp)) AS app_name,
  udf_mode_last(ARRAY_AGG(application.version ORDER BY submission_timestamp)) AS app_version,
  udf_json_mode_last(
    ARRAY_AGG(
      STRUCT(
        environment.settings.attribution.source,
        environment.settings.attribution.medium,
        environment.settings.attribution.campaign,
        environment.settings.attribution.content
      ) ORDER BY submission_timestamp
    )
  ) AS attribution,
  udf_mode_last(ARRAY_AGG(environment.settings.blocklist_enabled ORDER BY submission_timestamp)) AS blocklist_enabled,
  udf_mode_last(ARRAY_AGG(metadata.uri.app_update_channel ORDER BY submission_timestamp)) AS channel,
  AVG(_client_clock_skew) AS client_clock_skew_mean,
  AVG(_client_submission_latency) AS client_submission_latency_mean,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.cores ORDER BY submission_timestamp)) AS cpu_cores,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.count ORDER BY submission_timestamp)) AS cpu_count,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.family ORDER BY submission_timestamp)) AS cpu_family,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.cpu.l2cache_kb AS INT64) ORDER BY submission_timestamp)) AS cpu_l2_cache_kb,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.cpu.l3cache_kb AS INT64) ORDER BY submission_timestamp)) AS cpu_l3_cache_kb,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.model ORDER BY submission_timestamp)) AS cpu_model,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.cpu.speed_m_hz AS INT64) ORDER BY submission_timestamp)) AS cpu_speed_mhz,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.stepping ORDER BY submission_timestamp)) AS cpu_stepping,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.vendor ORDER BY submission_timestamp)) AS cpu_vendor,
  SUM(_crashes_detected.content) AS crashes_detected_content_sum,
  SUM(_crashes_detected.gmplugin) AS crashes_detected_gmplugin_sum,
  SUM(_crashes_detected.plugin) AS crashes_detected_plugin_sum,
  SUM(_crash_submit_attempt.content) AS crash_submit_attempt_content_sum,
  SUM(_crash_submit_attempt.main) AS crash_submit_attempt_main_sum,
  SUM(_crash_submit_attempt.plugin) AS crash_submit_attempt_plugin_sum,
  SUM(_crash_submit_success.content) AS crash_submit_success_content_sum,
  SUM(_crash_submit_success.main) AS crash_submit_success_main_sum,
  SUM(_crash_submit_success.plugin) AS crash_submit_success_plugin_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine ORDER BY submission_timestamp)) AS default_search_engine,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.load_path ORDER BY submission_timestamp)) AS default_search_engine_data_load_path,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.name ORDER BY submission_timestamp)) AS default_search_engine_data_name,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.origin ORDER BY submission_timestamp)) AS default_search_engine_data_origin,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.submission_url ORDER BY submission_timestamp)) AS default_search_engine_data_submission_url,
  SUM(udf_json_extract_histogram(payload.histograms.devtools_toolbox_opened_count).sum) AS devtools_toolbox_opened_count_sum,
  udf_mode_last(ARRAY_AGG(environment.partner.distribution_id ORDER BY submission_timestamp)) AS distribution_id,
  udf_mode_last(ARRAY_AGG(environment.settings.e10s_enabled ORDER BY submission_timestamp)) AS e10s_enabled,
  udf_mode_last(ARRAY_AGG(environment.build.architecture ORDER BY submission_timestamp)) AS env_build_arch,
  udf_mode_last(ARRAY_AGG(environment.build.build_id ORDER BY submission_timestamp)) AS env_build_id,
  udf_mode_last(ARRAY_AGG(environment.build.version ORDER BY submission_timestamp)) AS env_build_version,
  udf_json_mode_last(ARRAY_AGG(STRUCT(udf_null_if_empty_list(environment.settings.intl.accept_languages) AS list) ORDER BY submission_timestamp)).list AS environment_settings_intl_accept_languages,
  udf_json_mode_last(ARRAY_AGG(STRUCT(udf_null_if_empty_list(environment.settings.intl.app_locales) AS list) ORDER BY submission_timestamp)).list AS environment_settings_intl_app_locales,
  udf_json_mode_last(ARRAY_AGG(STRUCT(udf_null_if_empty_list(environment.settings.intl.available_locales) AS list) ORDER BY submission_timestamp)).list AS environment_settings_intl_available_locales,
  udf_json_mode_last(ARRAY_AGG(STRUCT(udf_null_if_empty_list(environment.settings.intl.requested_locales) AS list) ORDER BY submission_timestamp)).list AS environment_settings_intl_requested_locales,
  udf_json_mode_last(ARRAY_AGG(STRUCT(udf_null_if_empty_list(environment.settings.intl.system_locales) AS list) ORDER BY submission_timestamp)).list AS environment_settings_intl_system_locales,
  udf_json_mode_last(ARRAY_AGG(STRUCT(udf_null_if_empty_list(environment.settings.intl.regional_prefs_locales) AS list) ORDER BY submission_timestamp)).list AS environment_settings_intl_regional_prefs_locales,
  udf_map_mode_last(ARRAY_CONCAT_AGG(_experiments ORDER BY submission_timestamp)) AS experiments,
  AVG(_first_paint) AS first_paint_mean,
  udf_mode_last(ARRAY_AGG(_flash_version ORDER BY submission_timestamp)) AS flash_version,
  udf_json_mode_last(ARRAY_AGG(udf_geo_struct(metadata.geo.country, metadata.geo.city, metadata.geo.subdivision1, metadata.geo.subdivision2) ORDER BY submission_timestamp)).*,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.advanced_layers.status ORDER BY submission_timestamp)) AS gfx_features_advanced_layers_status,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.d2d.status ORDER BY submission_timestamp)) AS gfx_features_d2d_status,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.d3d11.status ORDER BY submission_timestamp)) AS gfx_features_d3d11_status,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.gpu_process.status ORDER BY submission_timestamp)) AS gfx_features_gpu_process_status,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_aboutdebugging_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_aboutdebugging_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_animationinspector_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_animationinspector_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_browserconsole_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_browserconsole_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_canvasdebugger_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_canvasdebugger_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_computedview_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_computedview_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_custom_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_custom_opened_count_sum,
  NULL AS histogram_parent_devtools_developertoolbar_opened_count_sum, -- deprecated
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_dom_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_dom_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_eyedropper_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_eyedropper_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_fontinspector_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_fontinspector_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_inspector_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_inspector_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_jsbrowserdebugger_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_jsbrowserdebugger_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_jsdebugger_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_jsdebugger_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_jsprofiler_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_jsprofiler_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_layoutview_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_layoutview_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_memory_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_memory_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_menu_eyedropper_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_menu_eyedropper_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_netmonitor_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_netmonitor_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_options_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_options_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_paintflashing_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_paintflashing_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_picker_eyedropper_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_picker_eyedropper_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_responsive_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_responsive_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_ruleview_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_ruleview_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_scratchpad_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_scratchpad_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_scratchpad_window_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_scratchpad_window_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_shadereditor_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_shadereditor_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_storage_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_storage_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_styleeditor_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_styleeditor_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_webaudioeditor_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_webaudioeditor_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_webconsole_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_webconsole_opened_count_sum,
  SUM(SAFE_CAST(JSON_EXTRACT(payload.histograms.devtools_webide_opened_count, '$.values.0') AS INT64)) AS histogram_parent_devtools_webide_opened_count_sum,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.install_year AS INT64) ORDER BY submission_timestamp)) AS install_year,
  udf_mode_last(ARRAY_AGG(environment.settings.is_default_browser ORDER BY submission_timestamp)) AS is_default_browser,
  udf_mode_last(ARRAY_AGG(environment.system.is_wow64 ORDER BY submission_timestamp)) AS is_wow64,
  udf_mode_last(ARRAY_AGG(environment.settings.locale ORDER BY submission_timestamp)) AS locale,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.memory_mb AS INT64) ORDER BY submission_timestamp)) AS memory_mb,
  udf_mode_last(ARRAY_AGG(normalized_channel ORDER BY submission_timestamp)) AS normalized_channel,
  udf_mode_last(ARRAY_AGG(normalized_os_version ORDER BY submission_timestamp)) AS normalized_os_version,
  udf_mode_last(ARRAY_AGG(environment.system.os.name ORDER BY submission_timestamp)) AS os,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.service_pack_major AS INT64) ORDER BY submission_timestamp)) AS os_service_pack_major,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.service_pack_minor AS INT64) ORDER BY submission_timestamp)) AS os_service_pack_minor,
  udf_mode_last(ARRAY_AGG(environment.system.os.version ORDER BY submission_timestamp)) AS os_version,
  COUNT(*) AS pings_aggregated_by_this_row,
  AVG(udf_histogram_to_mean(udf_json_extract_histogram(payload.histograms.places_bookmarks_count))) AS places_bookmarks_count_mean,
  AVG(udf_histogram_to_mean(udf_json_extract_histogram(payload.histograms.places_pages_count))) AS places_pages_count_mean,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "pluginhang")).sum) AS plugin_hangs_sum,
  SUM(udf_json_extract_histogram(payload.histograms.plugins_infobar_allow).sum) AS plugins_infobar_allow_sum,
  SUM(udf_json_extract_histogram(payload.histograms.plugins_infobar_block).sum) AS plugins_infobar_block_sum,
  SUM(udf_json_extract_histogram(payload.histograms.plugins_infobar_shown).sum) AS plugins_infobar_shown_sum,
  SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_user_action, "$.values.1") AS INT64)) AS plugins_notification_shown_sum,
  udf_mode_last(ARRAY_AGG(payload.info.previous_build_id ORDER BY submission_timestamp)) AS previous_build_id,
  UNIX_DATE(DATE(SAFE.TIMESTAMP(ANY_VALUE(payload.info.subsession_start_date)))) - ANY_VALUE(SAFE_CAST(environment.profile.creation_date AS INT64)) AS profile_age_in_days,
  FORMAT_DATE("%F 00:00:00", SAFE.DATE_FROM_UNIX_DATE(ANY_VALUE(SAFE_CAST(environment.profile.creation_date AS INT64)))) AS profile_creation_date,
  SUM(udf_json_extract_histogram(payload.histograms.push_api_notify).sum) AS push_api_notify_sum,
  ANY_VALUE(sample_id) AS sample_id,
  udf_mode_last(ARRAY_AGG(environment.settings.sandbox.effective_content_process_level ORDER BY submission_timestamp)) AS sandbox_effective_content_process_level,
  IFNULL(SUM(payload.processes.parent.scalars.webrtc_nicer_stun_retransmits), 0) + IFNULL(SUM(payload.processes.content.scalars.webrtc_nicer_stun_retransmits), 0) AS scalar_combined_webrtc_nicer_stun_retransmits_sum,
  IFNULL(SUM(payload.processes.parent.scalars.webrtc_nicer_turn_401s), 0) + IFNULL(SUM(payload.processes.content.scalars.webrtc_nicer_turn_401s), 0) AS scalar_combined_webrtc_nicer_turn_401s_sum,
  IFNULL(SUM(payload.processes.parent.scalars.webrtc_nicer_turn_403s), 0) + IFNULL(SUM(payload.processes.content.scalars.webrtc_nicer_turn_403s), 0) AS scalar_combined_webrtc_nicer_turn_403s_sum,
  IFNULL(SUM(payload.processes.parent.scalars.webrtc_nicer_turn_438s), 0) + IFNULL(SUM(payload.processes.content.scalars.webrtc_nicer_turn_438s), 0) AS scalar_combined_webrtc_nicer_turn_438s_sum,
  SUM(payload.processes.content.scalars.navigator_storage_estimate_count) AS scalar_content_navigator_storage_estimate_count_sum,
  SUM(payload.processes.content.scalars.navigator_storage_persist_count) AS scalar_content_navigator_storage_persist_count_sum,
  udf_mode_last(ARRAY_AGG(payload.processes.parent.scalars.aushelper_websense_reg_version ORDER BY submission_timestamp)) AS scalar_parent_aushelper_websense_reg_version,
  MAX(payload.processes.parent.scalars.browser_engagement_max_concurrent_tab_count) AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
  MAX(payload.processes.parent.scalars.browser_engagement_max_concurrent_window_count) AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
  SUM(payload.processes.parent.scalars.browser_engagement_tab_open_event_count) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
  SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count) AS scalar_parent_browser_engagement_total_uri_count_sum,
  SUM(payload.processes.parent.scalars.browser_engagement_unfiltered_uri_count) AS scalar_parent_browser_engagement_unfiltered_uri_count_sum,
  MAX(payload.processes.parent.scalars.browser_engagement_unique_domains_count) AS scalar_parent_browser_engagement_unique_domains_count_max,
  AVG(payload.processes.parent.scalars.browser_engagement_unique_domains_count) AS scalar_parent_browser_engagement_unique_domains_count_mean,
  SUM(payload.processes.parent.scalars.browser_engagement_window_open_event_count) AS scalar_parent_browser_engagement_window_open_event_count_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_node_inspected_count) AS scalar_parent_devtools_accessibility_node_inspected_count_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_opened_count) AS scalar_parent_devtools_accessibility_opened_count_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_picker_used_count) AS scalar_parent_devtools_accessibility_picker_used_count_sum,
  udf_map_sum(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node ORDER BY submission_timestamp)) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_service_enabled_count) AS scalar_parent_devtools_accessibility_service_enabled_count_sum,
  SUM(payload.processes.parent.scalars.devtools_copy_full_css_selector_opened) AS scalar_parent_devtools_copy_full_css_selector_opened_sum,
  SUM(payload.processes.parent.scalars.devtools_copy_unique_css_selector_opened) AS scalar_parent_devtools_copy_unique_css_selector_opened_sum,
  SUM(payload.processes.parent.scalars.devtools_toolbar_eyedropper_opened) AS scalar_parent_devtools_toolbar_eyedropper_opened_sum,
  NULL AS scalar_parent_dom_contentprocess_troubled_due_to_memory_sum, -- deprecated
  SUM(payload.processes.parent.scalars.navigator_storage_estimate_count) AS scalar_parent_navigator_storage_estimate_count_sum,
  SUM(payload.processes.parent.scalars.navigator_storage_persist_count) AS scalar_parent_navigator_storage_persist_count_sum,
  SUM(payload.processes.parent.scalars.storage_sync_api_usage_extensions_using) AS scalar_parent_storage_sync_api_usage_extensions_using_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.search_cohort ORDER BY submission_timestamp)) AS search_cohort,
  udf_aggregate_search_counts(ARRAY_CONCAT_AGG(udf_search_counts(payload.keyed_histograms.search_counts) ORDER BY submission_timestamp)).*,
  AVG(payload.simple_measurements.session_restored) AS session_restored_mean,
  COUNTIF(payload.info.subsession_counter = 1) AS sessions_started_on_this_day,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_kill_hard, "shut_down_kill")).sum) AS shutdown_kill_sum,
  SUM(payload.info.subsession_length/NUMERIC '3600') AS subsession_hours_sum,
  SUM((SELECT SUM(value) FROM UNNEST(udf_json_extract_histogram(payload.histograms.ssl_handshake_result).values) WHERE key BETWEEN 1 AND 671)) AS ssl_handshake_result_failure_sum,
  SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.ssl_handshake_result, "$.values.0") AS INT64)) AS ssl_handshake_result_success_sum,
  udf_mode_last(ARRAY_AGG(udf_boolean_histogram_to_boolean(payload.histograms.weave_configured) ORDER BY submission_timestamp)) AS sync_configured,
  AVG(_sync_count_desktop) AS sync_count_desktop_mean,
  AVG(_sync_count_mobile) AS sync_count_mobile_mean,
  SUM(_sync_count_desktop) AS sync_count_desktop_sum,
  SUM(_sync_count_mobile) AS sync_count_mobile_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.telemetry_enabled ORDER BY submission_timestamp)) AS telemetry_enabled,
  udf_mode_last(ARRAY_AGG(payload.info.timezone_offset ORDER BY submission_timestamp)) AS timezone_offset,
  CAST(NULL AS NUMERIC) AS total_hours_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.update.auto_download ORDER BY submission_timestamp)) AS update_auto_download,
  udf_mode_last(ARRAY_AGG(environment.settings.update.channel ORDER BY submission_timestamp)) AS update_channel,
  udf_mode_last(ARRAY_AGG(environment.settings.update.enabled ORDER BY submission_timestamp)) AS update_enabled,
  udf_mode_last(ARRAY_AGG(application.vendor ORDER BY submission_timestamp)) AS vendor,
  SUM(udf_json_extract_histogram(payload.histograms.push_api_notify).sum) AS web_notification_shown_sum,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.windows_build_number AS INT64) ORDER BY submission_timestamp)) AS windows_build_number,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.windows_ubr AS INT64) ORDER BY submission_timestamp)) AS windows_ubr
FROM
  pings
LEFT JOIN
  overactive
USING
  (client_id)
WHERE
  DATE(submission_timestamp) = @submission_date
  -- filter out overactive client_ids to prevent OOM errors
  AND overactive.client_id IS NULL
GROUP BY
  client_id,
  submission_date
