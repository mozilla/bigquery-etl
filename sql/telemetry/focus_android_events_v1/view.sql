CREATE OR REPLACE VIEW
    `moz-fx-data-shared-prod.telemetry.focus_android_events_v1` AS
WITH base_events AS (

SELECT
  *,
  DATE(submission_timestamp) AS submission_date,
  event.f0_ AS timestamp,
  event.f0_ AS event_timestamp,
  event.f1_ AS event_category,
  event.f2_ AS event_method,
  event.f3_ AS event_object,
  event.f4_ AS event_value,
  event.f5_ AS event_map_values,
  metadata.uri.app_version,
  osversion AS os_version,
  metadata.geo.country
FROM
  `moz-fx-data-shared-prod.telemetry.focus_event`
  CROSS JOIN UNNEST(events) AS event

), all_events AS (
SELECT
    submission_date,
    submission_timestamp,
    client_id AS device_id,
    `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'session_id') AS session_id_offset,
    CASE
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tip') ) AND (event_value IN ('open_in_new_tab_tip') ) THEN 'Focus - Open in new tab tip displayed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tip') ) AND (event_value IN ('add_to_homescreen_tip') ) THEN 'Focus - Add to homescreen tip displayed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tip') ) AND (event_value IN ('disable_tracking_protection_tip') ) THEN 'Focus - Disable tracking protection tip displayed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tip') ) AND (event_value IN ('default_browser_tip') ) THEN 'Focus - Set default browser tip displayed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tip') ) AND (event_value IN ('add_autocomplete_url_tip') ) THEN 'Focus - Autocomplete URL tip displayed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('tip') ) AND (event_value IN ('open_in_new_tab_tip') ) THEN 'Focus - Open in new tab tip tapped' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('tip') ) AND (event_value IN ('add_to_homescreen_tip') ) THEN 'Focus - Add to homescreen tip tapped' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('tip') ) AND (event_value IN ('disable_tracking_protection_tip') ) THEN 'Focus - Disable tracking protection tip tapped' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('tip') ) AND (event_value IN ('default_browser_tip') ) THEN 'Focus - Set default browser tip tapped' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('tip') ) AND (event_value IN ('add_autocomplete_url_tip') ) THEN 'Focus - Autocomplete URL tip tapped' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('find_in_page') ) THEN 'Focus - Find in Page Clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('desktop_request_check') ) THEN 'Focus - Request Desktop Site Clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('save') ) AND (event_object IN ('autocomplete_domain') ) THEN 'Focus - Autocomplete Updates' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('foreground') ) AND (event_object IN ('app') ) THEN 'Focus - Start session' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('background') ) AND (event_object IN ('app') ) THEN 'Focus - Stop session' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('type_url') ) AND (event_object IN ('search_bar') ) THEN 'Focus - URL entered' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('type_query') ) AND (event_object IN ('search_bar') ) THEN 'Focus - Search entered' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('select_query') ) AND (event_object IN ('search_bar') ) THEN 'Focus - Search hint clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('intent_url', 'text_selection_intent') ) AND (event_object IN ('app') ) THEN 'Focus - Link from third-party app' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('swipe') ) AND (event_object IN ('browser') ) AND (event_value IN ('reload') ) THEN 'Focus - Pull to refresh' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('intent_custom_tab') ) AND (event_object IN ('app') ) THEN 'Focus - Custom tab opened' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('custom_tab_close_bu') ) THEN 'Focus - Custom Tab - Close button clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('custom_tab_action_bu') ) THEN 'Focus - Custom Tab - Action button clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('menu') ) AND (event_value IN ('custom_tab') ) THEN 'Focus - Custom Tab - Menu item selected' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('full_browser') ) THEN 'Focus - Custom Tab - Open full browser' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('cancel') ) AND (event_object IN ('browser_contextmenu') ) THEN 'Focus - Context menu dismissed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share', 'copy') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('link') ) THEN 'Focus - Link Context Menu - Item selected' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share', 'copy', 'save') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('image') ) THEN 'Focus - Image Context Menu - Item selected' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('browser_contextmenu') ) AND (event_value IN ('tab') ) THEN 'Focus - Open link in new tab' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('erase_button') ) THEN 'Focus - Floating erase button clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('back_button') ) AND (event_value IN ('erase_home', 'erase_app') ) THEN 'Focus - Back button clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('shortcut') ) AND (event_value IN ('erase') ) THEN 'Focus - Home screen shortcut clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('tabs_tray') ) AND (event_value IN ('erase') ) THEN 'Focus - Erase history in tabs tray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('menu') ) AND (event_value IN ('default', 'firefox', 'selection') ) THEN 'Focus - Open URL Outside Focus' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('install') ) AND (event_object IN ('app') ) AND (event_value IN ('firefox') ) THEN 'Focus - Click "Download Firefox"' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('whats_new') ) THEN 'Focus - Click "What\'s New"' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('app_icon', 'homescreen_shortcut') ) AND (event_value IN ('open', 'resume') ) THEN 'Focus - Start Focus' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('save') ) AND (event_object IN ('autocomplete_domain') ) THEN 'Focus - Autocomplete domain added' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('autocomplete_domain') ) THEN 'Focus - Autocomplete domain removed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('reorder') ) AND (event_object IN ('autocomplete_domain') ) THEN 'Focus - Autocomplete domain reordered' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('open') ) AND (event_object IN ('search_engine_setting') ) THEN 'Focus - Default search engine clicked' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('save') ) AND (event_object IN ('search_engine_setting') ) THEN 'Focus - Change default search engine' 
        WHEN (event_category IN ('remove') ) AND (event_method IN ('remove') ) AND (event_object IN ('remove_search_engines') ) THEN 'Focus - Delete search engines' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('restore') ) AND (event_object IN ('search_engine_setting') ) THEN 'Focus - Restore bundled engines' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('custom_search_engine') ) THEN 'Focus - Select "Add another engine"' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('save') ) AND (event_object IN ('custom_search_engine') ) THEN 'Focus - Save custom search engine' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('search_engine_learn_more') ) THEN 'Focus - Click "Add search engine" Informational Button' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('firstrun') ) AND (event_value IN ('0') ) THEN 'Focus - Show first run' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('firstrun') ) AND (event_value IN ('skip') ) THEN 'Focus - Skip button pressed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('firstrun') ) AND (event_value IN ('finish') ) THEN 'Focus - Finish button pressed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('tabs_tray') ) THEN 'Focus - Open the tabs tray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('hide') ) AND (event_object IN ('tabs_tray') ) THEN 'Focus - Close the tabs tray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('tabs_tray') ) AND (event_value IN ('tab') ) THEN 'Focus - Switch to tab in tabs tray' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('long_press') ) AND (event_object IN ('browser') ) THEN 'Focus - Long press on image / link' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('show') ) AND (event_object IN ('autofill') ) THEN 'Focus - Autofill popup is shown' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('autofill') ) THEN 'Focus - Autofill performed' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('blocking_switch') ) AND (event_value IN ('true', 'false') ) THEN 'Focus - Content Blocking changed\n' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('share') ) AND (event_object IN ('menu') ) THEN 'Focus - Share URL with third-party app' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('reload') ) THEN 'Focus - Reload current page' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('download_dialog') ) AND (event_value IN ('download', 'cancel') ) THEN 'Focus - Download Dialog Shown' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('add_to_homescreen_dialog') ) AND (event_value IN ('add_to_homescreen', 'cancel') ) THEN 'Focus - "Add to Homescreen" dialog shown'
    END AS event_name,
    event_timestamp AS timestamp,
    app_version,
    os AS platform,
    os AS os_name,
    os_version,
    country,
    (SELECT
      ARRAY_AGG(CONCAT('"',
        CAST(key AS STRING), '":"',
        CAST(value AS STRING), '"'))
     FROM
       UNNEST(event_map_values)) AS event_props_1,
    event_map_values,
    event_object,
    event_value,
    event_method,
    event_category,
    created,
    settings
FROM
    base_events

), all_events_with_insert_ids AS (
SELECT
  * EXCEPT (event_category, created),
  CONCAT(device_id, "-", CAST(created AS STRING), "-", SPLIT(event_name, " - ")[OFFSET(1)], "-", CAST(timestamp AS STRING), "-", event_category, "-", event_method, "-", event_object) AS insert_id,
  event_name AS event_type
FROM
  all_events
WHERE
  event_name IS NOT NULL
), extra_props AS (
SELECT
  * EXCEPT (event_map_values, event_object, event_value, event_method),
  (SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
      SELECT 'requested' AS key, CASE
          WHEN event_name = 'Focus - Request Desktop Site Clicked' THEN event_value
          END AS value
      UNION ALL SELECT 'source' AS key, CASE
          WHEN event_name = 'Focus - Autocomplete Updates' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'source')
          END AS value
      UNION ALL SELECT 'intent_type' AS key, CASE
          WHEN event_name = 'Focus - Link from third-party app' THEN event_method
          END AS value
      UNION ALL SELECT 'item' AS key, CASE
          WHEN event_name = 'Focus - Link Context Menu - Item selected' THEN event_method
          WHEN event_name = 'Focus - Image Context Menu - Item selected' THEN event_method
          END AS value
      UNION ALL SELECT 'destination' AS key, CASE
          WHEN event_name = 'Focus - Back button clicked' THEN event_value
          END AS value
      UNION ALL SELECT 'app' AS key, CASE
          WHEN event_name = 'Focus - Open URL Outside Focus' THEN event_value
          END AS value
      UNION ALL SELECT 'start_type' AS key, CASE
          WHEN event_name = 'Focus - Start Focus' THEN event_value
          END AS value
      UNION ALL SELECT 'enabled' AS key, CASE
          WHEN event_name = 'Focus - Content Blocking changed\n' THEN event_value
          END AS value
      UNION ALL SELECT 'action' AS key, CASE
          WHEN event_name = 'Focus - Download Dialog Shown' THEN event_value
          WHEN event_name = 'Focus - "Add to Homescreen" dialog shown' THEN event_value
          END AS value
  ) WHERE VALUE IS NOT NULL) AS event_props_2,
  ARRAY<STRING>[] AS user_props
FROM
  all_events_with_insert_ids
)

SELECT
  * EXCEPT (event_props_1, event_props_2, user_props, settings),
  CONCAT('{', ARRAY_TO_STRING((
   SELECT ARRAY_AGG(DISTINCT e) FROM UNNEST(ARRAY_CONCAT(event_props_1, event_props_2)) AS e
  ), ","), '}') AS event_properties,
  CONCAT('{', ARRAY_TO_STRING(user_props, ","), '}') AS user_properties
FROM extra_props
