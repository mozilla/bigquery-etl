CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_broken_site_report.user_reports`
AS
WITH historical_reports AS (
  -- get old reports; already deduplicated
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.broken_site_report_v1`
  WHERE
    DATE(submission_timestamp) > "2023-11-01"
),
live_reports AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY submission_timestamp) AS rn
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.broken_site_report_v1`
  WHERE
    -- only get reports from the live tables that haven't gotten materialized in the stable tables
    DATE(submission_timestamp) > (SELECT DATE(MAX(submission_timestamp)) FROM historical_reports)
    AND DATE(submission_timestamp) > "2023-11-01"
),
all_reports AS (
  -- merge historical and new reports; deduplicate reports from the live tables
  SELECT
    * EXCEPT (rn)
  FROM
    live_reports
  WHERE
    rn = 1
  UNION ALL
  SELECT
    *
  FROM
    historical_reports
)
SELECT
  document_id AS uuid,
  CAST(submission_timestamp AS DATETIME) AS reported_at,
  metrics.text2.broken_site_report_description AS comments,
  metrics.url2.broken_site_report_url AS url,
  metrics.string.broken_site_report_breakage_category AS breakage_category,
  TO_JSON_STRING(
    STRUCT(
      STRUCT(
        STRUCT(
          metrics.text2.broken_site_report_browser_info_app_default_useragent_string AS default_useragent_string,
          metrics.boolean.broken_site_report_browser_info_app_fission_enabled AS fission_enabled,
          metrics.string_list.broken_site_report_browser_info_app_default_locales AS app_default_locales
        ) AS app,
        STRUCT(
          metrics.text2.broken_site_report_browser_info_graphics_devices_json AS devices_json,
          metrics.text2.broken_site_report_browser_info_graphics_drivers_json AS drivers_json,
          metrics.text2.broken_site_report_browser_info_graphics_features_json AS features_json,
          metrics.text2.broken_site_report_browser_info_graphics_monitors_json AS monitors_json,
          metrics.boolean.broken_site_report_browser_info_graphics_has_touch_screen AS has_touch_screen,
          metrics.string.broken_site_report_browser_info_graphics_device_pixel_ratio AS device_pixel_ratio
        ) AS graphics,
        STRUCT(
          metrics.boolean.broken_site_report_browser_info_prefs_software_webrender AS software_webrender,
          metrics.boolean.broken_site_report_browser_info_prefs_global_privacy_control_enabled AS global_privacy_control_enabled,
          metrics.boolean.broken_site_report_browser_info_prefs_installtrigger_enabled AS installtrigger_enabled,
          metrics.boolean.broken_site_report_browser_info_prefs_forced_accelerated_layers AS forced_accelerated_layers,
          metrics.boolean.broken_site_report_browser_info_prefs_opaque_response_blocking AS opaque_response_blocking,
          metrics.boolean.broken_site_report_browser_info_prefs_resist_fingerprinting_enabled AS resist_fingerprinting_enabled,
          metrics.quantity.broken_site_report_browser_info_prefs_cookie_behavior AS cookie_behavior
        ) AS prefs,
        STRUCT(
          metrics.boolean.broken_site_report_browser_info_system_is_tablet AS is_tablet,
          metrics.quantity.broken_site_report_browser_info_system_memory AS memory
        ) AS system,
        STRUCT(
          metrics.string_list.broken_site_report_browser_info_security_antispyware AS antispyware,
          metrics.string_list.broken_site_report_browser_info_security_antivirus AS antivirus,
          metrics.string_list.broken_site_report_browser_info_security_firewall AS firewall
        ) AS security
      ) AS browser_info,
      STRUCT(
        metrics.text2.broken_site_report_tab_info_useragent_string AS useragent_string,
        metrics.string_list.broken_site_report_tab_info_languages AS languages,
        STRUCT(
          metrics.boolean.broken_site_report_tab_info_frameworks_mobify AS mobify,
          metrics.boolean.broken_site_report_tab_info_frameworks_fastclick AS fastclick,
          metrics.boolean.broken_site_report_tab_info_frameworks_marfeel AS marfeel
        ) AS frameworks,
        STRUCT(
          metrics.boolean.broken_site_report_tab_info_antitracking_has_mixed_display_content_blocked AS has_mixed_display_content_blocked,
          metrics.boolean.broken_site_report_tab_info_antitracking_is_private_browsing AS is_private_browsing,
          metrics.boolean.broken_site_report_tab_info_antitracking_has_mixed_active_content_blocked AS has_mixed_active_content_blocked,
          metrics.boolean.broken_site_report_tab_info_antitracking_has_tracking_content_blocked AS has_tracking_content_blocked,
          metrics.string.broken_site_report_tab_info_antitracking_block_list AS block_list
        ) AS antitracking
      ) AS tab_info
    )
  ) AS details
FROM
  all_reports
ORDER BY
  submission_timestamp ASC
