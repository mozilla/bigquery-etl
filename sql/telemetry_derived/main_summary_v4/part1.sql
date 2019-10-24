CREATE TEMP FUNCTION
  udf_boolean_histogram_to_boolean(histogram STRING) AS (
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(histogram,
          "$.values.1") AS INT64) > 0,
      NOT SAFE_CAST( JSON_EXTRACT_SCALAR(histogram,
          "$.values.0") AS INT64) > 0));
CREATE TEMP FUNCTION udf_get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
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
CREATE TEMP FUNCTION
  udf_histogram_max_key_with_nonzero_value(histogram STRING) AS ((
    SELECT
      MAX(key)
    FROM
      UNNEST(udf_json_extract_histogram(histogram).values)
    WHERE
      value > 0));
CREATE TEMP FUNCTION udf_histogram_to_mean(histogram ANY TYPE) AS (
  CASE
  WHEN histogram.sum < 0 THEN NULL
  WHEN histogram.sum = 0 THEN 0
  ELSE SAFE_CAST(TRUNC(histogram.sum / (SELECT SUM(value) FROM UNNEST(histogram.values) WHERE value > 0)) AS INT64)
  END
);
CREATE TEMP FUNCTION udf_histogram_to_threshold_count(histogram STRING, threshold INT64) AS (
  (
    SELECT
      IFNULL(SUM(value), 0)
    FROM
      UNNEST(udf_json_extract_histogram(histogram).values)
    WHERE
      key >= threshold
  )
);
CREATE TEMP FUNCTION udf_js_main_summary_addon_scalars(dynamic_scalars_json STRING, dynamic_keyed_scalars_json STRING)
RETURNS STRUCT<
  keyed_boolean_addon_scalars ARRAY<STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value BOOL>>>>,
  keyed_uint_addon_scalars ARRAY<STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value INT64>>>>,
  string_addon_scalars ARRAY<STRUCT<key STRING, value STRING>>,
  keyed_string_addon_scalars ARRAY<STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value STRING>>>>,
  uint_addon_scalars ARRAY<STRUCT<key STRING, value INT64>>,
  boolean_addon_scalars ARRAY<STRUCT<key STRING, value BOOL>>
>
LANGUAGE js AS """
try {
  const dynamicScalars = JSON.parse(dynamic_scalars_json) || {};
  const dynamicKeyedScalars = JSON.parse(dynamic_keyed_scalars_json) || {};
  const result = {
    keyed_boolean_addon_scalars: [],
    keyed_uint_addon_scalars: [],
    string_addon_scalars: [],
    keyed_string_addon_scalars: [],
    uint_addon_scalars: [],
    boolean_addon_scalars: [],
  };
  const typeMap = {
    number: 'uint',
    boolean: 'boolean',
    string: 'string',
  };
  Object.entries(dynamicKeyedScalars).forEach(([k, v]) => {
    const type = typeMap[typeof Object.values(v)[0]];
    const column = `keyed_${type}_addon_scalars`;
    result[column].push({
      key: k,
      value: Object.entries(v).map(([key, value]) => ({ key, value }))
    });
  });
  Object.entries(dynamicScalars).forEach(([k, v]) => {
    const type = typeMap[typeof v];
    result[`${type}_addon_scalars`].push({key: k, value: v});
  });
  return result;
} catch(err) {
  return null;
}
""";
CREATE TEMP FUNCTION udf_js_main_summary_disabled_addons(
  active_addon_ids ARRAY<STRING>,
  addon_details_json STRING
)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
try {
  const addonDetails = Object.keys(JSON.parse(addon_details_json) || {});
  return addonDetails.filter(k => !(active_addon_ids || []).includes(k));
} catch(err) {
  return null;
}
""";
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
SELECT
  document_id,
  client_id,
  sample_id,
  metadata.uri.app_update_channel AS channel,
  normalized_channel,
  normalized_os_version,
  metadata.geo.country,
  metadata.geo.city,
  metadata.geo.subdivision1 AS geo_subdivision1,
  metadata.geo.subdivision2 AS geo_subdivision2,
  environment.system.os.name AS os,
  environment.system.os.version AS os_version,
  SAFE_CAST(environment.system.os.service_pack_major AS INT64) AS os_service_pack_major,
  SAFE_CAST(environment.system.os.service_pack_minor AS INT64) AS os_service_pack_minor,
  SAFE_CAST(environment.system.os.windows_build_number AS INT64) AS windows_build_number,
  SAFE_CAST(environment.system.os.windows_ubr AS INT64) AS windows_ubr,

  -- Note: Windows only!
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
  environment.system.gfx.features.wr_qualified.status AS gfx_features_wrqualified_status,
  environment.system.gfx.features.webrender.status AS gfx_features_webrender_status,

  -- Bug 1552940
  environment.system.hdd.profile.type AS hdd_profile_type,
  environment.system.hdd.binary.type AS hdd_binary_type,
  environment.system.hdd.system.type AS hdd_system_type,

  environment.system.apple_model_id,

  -- Bug 1431198 - Windows 8 only
  environment.system.sec.antivirus,
  environment.system.sec.antispyware,
  environment.system.sec.firewall,

  -- TODO: use proper 'date' type for date columns.
  SAFE_CAST(environment.profile.creation_date AS INT64) AS profile_creation_date,
  SAFE_CAST(environment.profile.reset_date AS INT64) AS profile_reset_date,
  payload.info.previous_build_id,
  payload.info.session_id,
  payload.info.previous_session_id,
  payload.info.subsession_id,
  payload.info.previous_subsession_id,
  payload.info.session_start_date,
  payload.info.session_length,
  payload.info.subsession_length,
  payload.info.subsession_start_date,
  payload.info.subsession_counter,
  payload.info.profile_subsession_counter,
  creation_date,
  environment.partner.distribution_id,
  DATE(submission_timestamp) AS submission_date,
  -- See bug 1550752
  udf_boolean_histogram_to_boolean(payload.histograms.fxa_configured) AS fxa_configured,
  -- See bug 1232050
  udf_boolean_histogram_to_boolean(payload.histograms.weave_configured) AS sync_configured,
  udf_histogram_max_key_with_nonzero_value(payload.histograms.weave_device_count_desktop) AS sync_count_desktop,
  udf_histogram_max_key_with_nonzero_value(payload.histograms.weave_device_count_mobile) AS sync_count_mobile,

  application.build_id AS app_build_id,
  application.display_version AS app_display_version,
  application.name AS app_name,
  application.version AS app_version,
  UNIX_MICROS(submission_timestamp) * 1000 AS `timestamp`,

  environment.build.build_id AS env_build_id,
  environment.build.version AS env_build_version,
  environment.build.architecture AS env_build_arch,

  -- See bug 1232050
  environment.settings.e10s_enabled,
  environment.settings.e10s_cohort,

  -- See bug 1232050
  environment.settings.e10s_multi_processes,

  environment.settings.locale,
  environment.settings.update.channel AS update_channel,
  environment.settings.update.enabled AS update_enabled,
  environment.settings.update.auto_download AS update_auto_download,
  STRUCT(
    environment.settings.attribution.source,
    environment.settings.attribution.medium,
    environment.settings.attribution.campaign,
    environment.settings.attribution.content
  ) AS attribution,
  environment.settings.attribution.experiment AS attribution_experiment,
  environment.settings.attribution.variation AS attribution_variation,
  environment.settings.sandbox.effective_content_process_level AS sandbox_effective_content_process_level,
  environment.addons.active_experiment.id AS active_experiment_id,
  environment.addons.active_experiment.branch AS active_experiment_branch,
  payload.info.reason,

  payload.info.timezone_offset,

  -- Different types of crashes / hangs:
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'pluginhang')).sum AS plugin_hangs,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, 'plugin')).sum AS aborts_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, 'content')).sum AS aborts_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, 'gmplugin')).sum AS aborts_gmplugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin')).sum AS crashes_detected_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'content')).sum AS crashes_detected_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, 'gmplugin')).sum AS crashes_detected_gmplugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, 'main-crash')).sum AS crash_submit_attempt_main,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, 'content-crash')).sum AS crash_submit_attempt_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, 'plugin-crash')).sum AS crash_submit_attempt_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, 'main-crash')).sum AS crash_submit_success_main,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, 'content-crash')).sum AS crash_submit_success_content,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, 'plugin-crash')).sum AS crash_submit_success_plugin,
  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_kill_hard, 'ShutDownKill')).sum AS shutdown_kill,

  ARRAY_LENGTH(environment.addons.active_addons) AS active_addons_count,

  -- See https://github.com/mozilla-services/data-pipeline/blob/master/hindsight/modules/fx/ping.lua#L82
  udf_max_flash_version(environment.addons.active_plugins) AS flash_version, -- latest installable version of flash plugin
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

  -- DevTools usage per bug 1262478
  udf_json_extract_histogram(payload.histograms.devtools_toolbox_opened_count).sum AS devtools_toolbox_opened_count,

  -- client date per bug 1270505
  metadata.header.date AS client_submission_date, -- the HTTP Date header sent by the client

  -- clock skew per bug 1270183
  TIMESTAMP_DIFF(TIMESTAMP_TRUNC(submission_timestamp, SECOND), SAFE.PARSE_TIMESTAMP('%a, %d %b %Y %T %Z', metadata.header.date), SECOND) AS client_clock_skew,
  TIMESTAMP_DIFF(TIMESTAMP_TRUNC(submission_timestamp, SECOND), SAFE.PARSE_TIMESTAMP('%FT%R:%E*SZ', creation_date), SECOND) AS client_submission_latency,

  -- We use the mean for bookmarks and pages because we do not expect them to be
  -- heavily skewed during the lifetime of a subsession. Using the median for a
  -- histogram would probably be better in general, but the granularity of the
  -- buckets for these particular histograms is not fine enough for the median
  -- to give a more accurate value than the mean.
  udf_histogram_to_mean(udf_json_extract_histogram(payload.histograms.places_bookmarks_count)) AS places_bookmarks_count,
  udf_histogram_to_mean(udf_json_extract_histogram(payload.histograms.places_pages_count)) AS places_pages_count,

  -- Push metrics per bug 1270482 and bug 1311174
  udf_json_extract_histogram(payload.histograms.push_api_notify).sum AS push_api_notify,
  udf_json_extract_histogram(payload.histograms.web_notification_shown).sum AS web_notification_shown,

  -- Info from POPUP_NOTIFICATION_STATS keyed histogram
  ARRAY(
    SELECT AS STRUCT
      key,
      STRUCT(
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.0') AS INT64), 0) AS offered,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.1') AS INT64), 0) AS action_1,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.2') AS INT64), 0) AS action_2,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.3') AS INT64), 0) AS action_3,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.4') AS INT64), 0) AS action_last,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.5') AS INT64), 0) AS dismissal_click_elsewhere,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.6') AS INT64), 0) AS dismissal_leave_page,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.7') AS INT64), 0) AS dismissal_close_button,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.8') AS INT64), 0) AS dismissal_not_now,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.10') AS INT64), 0) AS open_submenu,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.11') AS INT64), 0) AS learn_more,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.20') AS INT64), 0) AS reopen_offered,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.21') AS INT64), 0) AS reopen_action_1,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.22') AS INT64), 0) AS reopen_action_2,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.23') AS INT64), 0) AS reopen_action_3,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.24') AS INT64), 0) AS reopen_action_last,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.25') AS INT64), 0) AS reopen_dismissal_click_elsewhere,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.26') AS INT64), 0) AS reopen_dismissal_leave_page,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.27') AS INT64), 0) AS reopen_dismissal_close_button,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.28') AS INT64), 0) AS reopen_dismissal_not_now,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.30') AS INT64), 0) AS reopen_open_submenu,
        IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(value, '$.values.31') AS INT64), 0) AS reopen_learn_more
      ) AS value
    FROM
      UNNEST(payload.keyed_histograms.popup_notification_stats)
  ) AS popup_notification_stats,

  -- Search counts
  -- split up and organize the SEARCH_COUNTS keyed histogram
  udf_search_counts(payload.keyed_histograms.search_counts) AS search_counts,

  -- Addon and configuration settings per Bug 1290181
  udf_main_summary_active_addons(environment.addons.active_addons) AS active_addons,

  -- Legacy/disabled addon and configuration settings per Bug 1390814. Please note that |disabled_addons_ids| may go away in the future.
  udf_js_main_summary_disabled_addons(
    ARRAY(SELECT key FROM UNNEST(environment.addons.active_addons)),
    JSON_EXTRACT(additional_properties, '$.payload.addonDetails.XPI')
  ) AS disabled_addons_ids, -- One per item in payload.addonDetails.XPI
  STRUCT(
    IFNULL(environment.addons.theme.id, 'MISSING') AS addon_id,
    environment.addons.theme.app_disabled,
    environment.addons.theme.blocklisted,
    SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, '$.environment.addons.theme.foreignInstall') AS BOOL) AS foreign_install,
    environment.addons.theme.has_binary_components,
    environment.addons.theme.install_day,
    -- define removed fields for schema compatiblity
    CAST(NULL AS BOOL) AS is_system,
    CAST(NULL AS BOOL) AS is_web_extension,
    CAST(NULL AS BOOL) AS multiprocess_compatible,
    environment.addons.theme.name,
    environment.addons.theme.scope,
    -- define removed fields for schema compatiblity
    NULL AS signed_state,
    CAST(NULL AS STRING) AS `type`,
    environment.addons.theme.update_day,
    environment.addons.theme.user_disabled,
    environment.addons.theme.version
  ) AS active_theme,
  environment.settings.blocklist_enabled,
  environment.settings.addon_compatibility_check_enabled,
  environment.settings.telemetry_enabled,

  environment.settings.intl.accept_languages AS environment_settings_intl_accept_languages,
  environment.settings.intl.app_locales AS environment_settings_intl_app_locales,
  environment.settings.intl.available_locales AS environment_settings_intl_available_locales,
  environment.settings.intl.regional_prefs_locales AS environment_settings_intl_regional_prefs_locales,
  environment.settings.intl.requested_locales AS environment_settings_intl_requested_locales,
  environment.settings.intl.system_locales AS environment_settings_intl_system_locales,

  environment.system.gfx.headless AS environment_system_gfx_headless,

  -- user prefs
  (
    SELECT AS STRUCT
      ANY_VALUE(IF(key = 'browser.launcherProcess.enabled', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_browser_launcherprocess_enabled,
      ANY_VALUE(IF(key = 'browser.search.widget.inNavBar', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_browser_search_widget_innavbar,
      ANY_VALUE(IF(key = 'browser.search.region', value, NULL)) AS user_pref_browser_search_region,
      ANY_VALUE(IF(key = 'extensions.allow-non-mpc-extensions', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_extensions_allow_non_mpc_extensions,
      ANY_VALUE(IF(key = 'extensions.legacy.enabled', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_extensions_legacy_enabled,
      ANY_VALUE(IF(key = 'gfx.webrender.all.qualified', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_gfx_webrender_all_qualified,
      ANY_VALUE(IF(key = 'marionette.enabled', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_marionette_enabled,
      ANY_VALUE(IF(key = 'privacy.fuzzyfox.enabled', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_privacy_fuzzyfox_enabled,
      ANY_VALUE(IF(key = 'dom.ipc.plugins.sandbox-level.flash', SAFE_CAST(value AS INT64), NULL)) AS user_pref_dom_ipc_plugins_sandbox_level_flash,
      ANY_VALUE(IF(key = 'dom.ipc.processCount', SAFE_CAST(value AS INT64), NULL)) AS user_pref_dom_ipc_processcount,
      ANY_VALUE(IF(key = 'general.config.filename', value, NULL)) AS user_pref_general_config_filename,
      ANY_VALUE(IF(key = 'security.enterprise_roots.auto-enabled', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_security_enterprise_roots_auto_enabled,
      ANY_VALUE(IF(key = 'security.enterprise_roots.enabled', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_security_enterprise_roots_enabled,
      ANY_VALUE(IF(key = 'security.pki.mitm_detected', SAFE_CAST(value AS BOOL), NULL)) AS user_pref_security_pki_mitm_detected,
      ANY_VALUE(IF(key = 'network.trr.mode', SAFE_CAST(value AS INT64), NULL)) AS user_pref_network_trr_mode,
      -- TODO: Deprecate and eventually remove this field, preferring the top-level user_pref_* fields for easy schema evolution.
      STRUCT(
        ANY_VALUE(IF(key = 'dom.ipc.process_count', SAFE_CAST(value AS INT64), NULL)) AS dom_ipc_process_count,
        ANY_VALUE(IF(key = 'extensions.allow-non_mpc-extensions', SAFE_CAST(value AS BOOL), NULL)) AS extensions_allow_non_mpc_extensions
      ) AS user_prefs
    FROM
      UNNEST(environment.settings.user_prefs)
  ).*,

  ARRAY(
    SELECT AS STRUCT
      f0_ AS `timestamp`,
      f1_ AS category,
      f2_ AS method,
      f3_ AS object,
      f4_ AS string_value,
      ARRAY_CONCAT(
        [STRUCT('telemetry_process' AS key, process AS value)],
        f5_
      ) AS map_values
    FROM
      UNNEST(
        [
          STRUCT('content' AS process, payload.processes.content.events),
          ('dynamic', payload.processes.dynamic.events),
          ('gpu', payload.processes.gpu.events),
          ('parent', payload.processes.parent.events)
        ]
      )
    CROSS JOIN
      UNNEST(events)
  ) AS events,

  -- bug 1339655
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.ssl_handshake_result, '$.values.0') AS INT64) AS ssl_handshake_result_success,
  (
    SELECT
      IFNULL(SUM(value), 0)
    FROM
      UNNEST(udf_json_extract_histogram(payload.histograms.ssl_handshake_result).values)
    WHERE
      key BETWEEN 1 AND 671
  ) AS ssl_handshake_result_failure,
  ARRAY(
    SELECT AS STRUCT
      CAST(key AS STRING) AS key,
      value
    FROM
      UNNEST(udf_json_extract_histogram(payload.histograms.ssl_handshake_result).values)
    WHERE
      key BETWEEN 0 AND 671
      AND value > 0
  ) AS ssl_handshake_result,

  -- bug 1353114 - payload.simpleMeasurements.*
  COALESCE(
    payload.processes.parent.scalars.browser_engagement_active_ticks,
    SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, '$.payload.simpleMeasurements.activeTicks') AS INT64)
  ) AS active_ticks,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, '$.payload.simpleMeasurements.main') AS INT64) AS main,
  COALESCE(
    payload.processes.parent.scalars.timestamps_first_paint,
    SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, '$.payload.simpleMeasurements.firstPaint') AS INT64)
  ) AS first_paint,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, '$.payload.simpleMeasurements.sessionRestored') AS INT64) AS session_restored,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, '$.payload.simpleMeasurements.totalTime') AS INT64) AS total_time,
  SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, '$.payload.simpleMeasurements.blankWindowShown') AS INT64) AS blank_window_shown,

  -- bug 1362520 and 1526278 - plugin notifications
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_shown, '$.values.1') AS INT64) AS plugins_notification_shown,
  SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_shown, '$.values.0') AS INT64) AS plugins_notification_shown_false,
  STRUCT(
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_user_action, '$.values.0') AS INT64) AS allow_now,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_user_action, '$.values.1') AS INT64) AS allow_always,
    SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_user_action, '$.values.2') AS INT64) AS block
  ) AS plugins_notification_user_action,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_shown).sum AS plugins_infobar_shown,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_block).sum AS plugins_infobar_block,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_allow).sum AS plugins_infobar_allow,
  udf_json_extract_histogram(payload.histograms.plugins_infobar_dismissed).sum AS plugins_infobar_dismissed,

  -- bug 1366253 - active experiments
  ARRAY(
    SELECT AS STRUCT
      key,
      value.branch AS value
    FROM
      UNNEST(environment.experiments)
  ) AS experiments,
  ARRAY(
    SELECT AS STRUCT
      key,
      [
        STRUCT('branch' AS key, value.branch AS value),
        ('type', value.type),
        ('enrollment_id', value.enrollment_id)
      ] AS value
    FROM
      UNNEST(environment.experiments)
  ) AS experiments_details,

  environment.settings.search_cohort,

  -- bug 1366838 - Quantum Release Criteria
  environment.system.gfx.features.compositor AS gfx_compositor,
  (
    environment.settings.e10s_enabled
    AND environment.addons.theme.id IN (
      '{972ce4c6-7e08-4474-a285-3208198ce6fd}',
      'firefox-compact-light@mozilla.org',
      'firefox-compact-dark@mozilla.org'
    )
    AND (
      SELECT
        LOGICAL_AND(
          value.is_system IS TRUE
          OR value.is_web_extension IS TRUE
        ) IS NOT FALSE
      FROM
        UNNEST(environment.addons.active_addons)
    )
  ) AS quantum_ready,

  udf_histogram_to_threshold_count(payload.histograms.gc_max_pause_ms_2, 150) AS gc_max_pause_ms_main_above_150,
  udf_histogram_to_threshold_count(payload.histograms.gc_max_pause_ms_2, 250) AS gc_max_pause_ms_main_above_250,
  udf_histogram_to_threshold_count(payload.histograms.gc_max_pause_ms_2, 2500) AS gc_max_pause_ms_main_above_2500,

  udf_histogram_to_threshold_count(payload.processes.content.histograms.gc_max_pause_ms_2, 150) AS gc_max_pause_ms_content_above_150,
  udf_histogram_to_threshold_count(payload.processes.content.histograms.gc_max_pause_ms_2, 250) AS gc_max_pause_ms_content_above_250,
  udf_histogram_to_threshold_count(payload.processes.content.histograms.gc_max_pause_ms_2, 2500) AS gc_max_pause_ms_content_above_2500,

  udf_histogram_to_threshold_count(payload.histograms.cycle_collector_max_pause, 150) AS cycle_collector_max_pause_main_above_150,
  udf_histogram_to_threshold_count(payload.histograms.cycle_collector_max_pause, 250) AS cycle_collector_max_pause_main_above_250,
  udf_histogram_to_threshold_count(payload.histograms.cycle_collector_max_pause, 2500) AS cycle_collector_max_pause_main_above_2500,

  udf_histogram_to_threshold_count(payload.processes.content.histograms.cycle_collector_max_pause, 150) AS cycle_collector_max_pause_content_above_150,
  udf_histogram_to_threshold_count(payload.processes.content.histograms.cycle_collector_max_pause, 250) AS cycle_collector_max_pause_content_above_250,
  udf_histogram_to_threshold_count(payload.processes.content.histograms.cycle_collector_max_pause, 2500) AS cycle_collector_max_pause_content_above_2500,

  udf_histogram_to_threshold_count(payload.histograms.input_event_response_coalesced_ms, 150) AS input_event_response_coalesced_ms_main_above_150,
  udf_histogram_to_threshold_count(payload.histograms.input_event_response_coalesced_ms, 250) AS input_event_response_coalesced_ms_main_above_250,
  udf_histogram_to_threshold_count(payload.histograms.input_event_response_coalesced_ms, 2500) AS input_event_response_coalesced_ms_main_above_2500,

  udf_histogram_to_threshold_count(payload.processes.content.histograms.input_event_response_coalesced_ms, 150) AS input_event_response_coalesced_ms_content_above_150,
  udf_histogram_to_threshold_count(payload.processes.content.histograms.input_event_response_coalesced_ms, 250) AS input_event_response_coalesced_ms_content_above_250,
  udf_histogram_to_threshold_count(payload.processes.content.histograms.input_event_response_coalesced_ms, 2500) AS input_event_response_coalesced_ms_content_above_2500,

  udf_histogram_to_threshold_count(payload.histograms.ghost_windows, 1) AS ghost_windows_main_above_1,
  udf_histogram_to_threshold_count(payload.processes.content.histograms.ghost_windows, 1) AS ghost_windows_content_above_1,

  udf_js_main_summary_addon_scalars(
    JSON_EXTRACT(additional_properties, '$.payload.processes.dynamic.scalars'),
    JSON_EXTRACT(additional_properties, '$.payload.processes.dynamic.keyedScalars')
  ).*,

  -- define removed fields for schema compatiblity
  NULL AS push_api_notification_received,
  STRUCT(
    NULL AS open_conversation,
    NULL AS open_panel,
    NULL AS room_delete,
    NULL AS room_open,
    NULL AS room_share
  ) AS loop_activity_counter
FROM
  `moz-fx-data-shared-prod.telemetry_stable.main_v4`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_app_name = 'Firefox'
  AND document_id IS NOT NULL
