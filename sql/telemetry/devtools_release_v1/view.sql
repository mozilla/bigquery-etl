CREATE OR REPLACE VIEW
    `moz-fx-data-shared-prod.telemetry.devtools_release_v1` AS
WITH base_events AS (

SELECT
  *,
  timestamp AS submission_timestamp,
  event_string_value AS event_value,
  session_start_time AS created,
  NULL AS city
FROM
  `moz-fx-data-shared-prod.telemetry.events`

), all_events AS (
SELECT
    submission_date,
    submission_timestamp,
    client_id AS device_id,
    (created + COALESCE(SAFE_CAST(`moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'session_id') AS NUMERIC), 0)) AS session_id,
    CASE
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('open') ) AND (event_object IN ('tools') ) THEN 'dt - open' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('close') ) AND (event_object IN ('tools') ) THEN 'dt - close' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('enter') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - enter' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('exit') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - exit' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('execute_js') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - execute_js' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('filters_changed') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - filters_changed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('jump_to_definition') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - jump_to_definition' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('jump_to_source') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - jump_to_source' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('object_expanded') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - object_expanded' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('persist_changed') ) AND (event_object IN ('webconsole') ) THEN 'dt_webconsole - persist_changed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('enter') ) AND (event_object IN ('inspector') ) THEN 'dt_inspector - enter' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('exit') ) AND (event_object IN ('inspector') ) THEN 'dt_inspector - exit' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('edit_html') ) AND (event_object IN ('inspector') ) THEN 'dt_inspector - edit_html' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('sidepanel_changed') ) AND (event_object IN ('inspector') ) THEN 'dt_inspector - sidepanel_changed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('enter') ) AND (event_object IN ('jsdebugger') ) THEN 'dt_jsdebugger - enter' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('exit') ) AND (event_object IN ('jsdebugger') ) THEN 'dt_jsdebugger - exit' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('add_breakpoint') ) AND (event_object IN ('debugger') ) THEN 'dt_jsdebugger - add_breakpoint' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('blackbox') ) AND (event_object IN ('debugger') ) THEN 'dt_jsdebugger - blackbox' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('continue') ) AND (event_object IN ('debugger') ) THEN 'dt_jsdebugger - continue' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('pause_on_exceptions') ) AND (event_object IN ('debugger') ) THEN 'dt_jsdebugger - pause_on_exceptions' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('pause') ) AND (event_object IN ('debugger') ) THEN 'dt_jsdebugger - pause' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('pretty_print') ) AND (event_object IN ('debugger') ) THEN 'dt_jsdebugger - pretty_print' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('remove_breakpoint') ) AND (event_object IN ('debugger') ) THEN 'dt_jsdebugger - remove_breakpoint' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('enter') ) AND (event_object IN ('styleeditor') ) THEN 'dt_styleeditor - enter' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('exit') ) AND (event_object IN ('styleeditor') ) THEN 'dt_styleeditor - exit' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('enter') ) AND (event_object IN ('netmonitor') ) THEN 'dt_netmonitor - enter' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('exit') ) AND (event_object IN ('netmonitor') ) THEN 'dt_netmonitor - exit' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('edit_resend') ) AND (event_object IN ('netmonitor') ) THEN 'dt_netmonitor - edit_resend' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('filters_changed') ) AND (event_object IN ('netmonitor') ) THEN 'dt_netmonitor - filters_changed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('persist_changed') ) AND (event_object IN ('netmonitor') ) THEN 'dt_netmonitor - persist_changed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('sidepanel_changed') ) AND (event_object IN ('netmonitor') ) THEN 'dt_netmonitor - sidepanel_changed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('throttle_changed') ) AND (event_object IN ('netmonitor') ) THEN 'dt_netmonitor - throttle_changed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('enter') ) AND (event_object IN ('storage') ) THEN 'dt_storage - enter' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('exit') ) AND (event_object IN ('storage') ) THEN 'dt_storage - exit' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('enter') ) AND (event_object IN ('other') ) THEN 'dt_other - enter' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('exit') ) AND (event_object IN ('other') ) THEN 'dt_other - exit' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('activate') ) AND (event_object IN ('responsive_design') ) THEN 'dt_responsive_design - activate' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('deactivate') ) AND (event_object IN ('responsive_design') ) THEN 'dt_responsive_design - deactivate' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('activate') ) AND (event_object IN ('split_console') ) THEN 'dt_split_console - activate' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('deactivate') ) AND (event_object IN ('split_console') ) THEN 'dt_split_console - deactivate' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('close_adbg') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - close_adbg' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('device_added') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - device_added' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('device_removed') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - device_removed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('inspect') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - inspect' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('open_adbg') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - open_adbg' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('runtime_added') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - runtime_added' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('runtime_connected') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - runtime_connected' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('runtime_disconnected') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - runtime_disconnected' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('runtime_removed') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - runtime_removed' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('select_page') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - select_page' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('show_profiler') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - show_profiler' 
        WHEN (event_category IN ('devtools.main') ) AND (event_method IN ('update_conn_prompt') ) AND (event_object IN ('aboutdebugging') ) THEN 'dt_adbg - update_conn_prompt'
    END AS event_name,
    event_timestamp AS timestamp,
    app_version,
    os AS platform,
    os AS os_name,
    os_version,
    country,
    city,
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
    NULL AS settings
FROM
    base_events
WHERE doc_type IN ('event')
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
      SELECT 'entrypoint' AS key, CASE
          WHEN event_name = 'dt - open' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'entrypoint')
          END AS value
      UNION ALL SELECT 'first_panel' AS key, CASE
          WHEN event_name = 'dt - open' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'first_panel')
          END AS value
      UNION ALL SELECT 'splitconsole' AS key, CASE
          WHEN event_name = 'dt - open' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'splitconsole')
          END AS value
      UNION ALL SELECT 'shortcut' AS key, CASE
          WHEN event_name = 'dt - open' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'shortcut')
          END AS value
      UNION ALL SELECT 'cold' AS key, CASE
          WHEN event_name = 'dt_webconsole - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'cold')
          WHEN event_name = 'dt_inspector - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'cold')
          WHEN event_name = 'dt_jsdebugger - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'cold')
          WHEN event_name = 'dt_styleeditor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'cold')
          WHEN event_name = 'dt_netmonitor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'cold')
          WHEN event_name = 'dt_storage - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'cold')
          WHEN event_name = 'dt_other - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'cold')
          END AS value
      UNION ALL SELECT 'message_count' AS key, CASE
          WHEN event_name = 'dt_webconsole - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'message_count')
          END AS value
      UNION ALL SELECT 'next_panel' AS key, CASE
          WHEN event_name = 'dt_webconsole - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'next_panel')
          WHEN event_name = 'dt_inspector - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'next_panel')
          WHEN event_name = 'dt_jsdebugger - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'next_panel')
          WHEN event_name = 'dt_styleeditor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'next_panel')
          WHEN event_name = 'dt_netmonitor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'next_panel')
          WHEN event_name = 'dt_storage - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'next_panel')
          WHEN event_name = 'dt_other - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'next_panel')
          END AS value
      UNION ALL SELECT 'reason' AS key, CASE
          WHEN event_name = 'dt_webconsole - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'dt_inspector - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'dt_jsdebugger - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'dt_jsdebugger - pause' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'dt_styleeditor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'dt_netmonitor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'dt_storage - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'dt_other - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          END AS value
      UNION ALL SELECT 'lines' AS key, CASE
          WHEN event_name = 'dt_webconsole - execute_js' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'lines')
          END AS value
      UNION ALL SELECT 'trigger' AS key, CASE
          WHEN event_name = 'dt_webconsole - filters_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'trigger')
          WHEN event_name = 'dt_netmonitor - filters_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'trigger')
          END AS value
      UNION ALL SELECT 'active' AS key, CASE
          WHEN event_name = 'dt_webconsole - filters_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'active')
          WHEN event_name = 'dt_netmonitor - filters_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'active')
          END AS value
      UNION ALL SELECT 'inactive' AS key, CASE
          WHEN event_name = 'dt_webconsole - filters_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'inactive')
          WHEN event_name = 'dt_netmonitor - filters_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'inactive')
          END AS value
      UNION ALL SELECT 'start_state' AS key, CASE
          WHEN event_name = 'dt_inspector - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'start_state')
          WHEN event_name = 'dt_jsdebugger - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'start_state')
          WHEN event_name = 'dt_styleeditor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'start_state')
          WHEN event_name = 'dt_netmonitor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'start_state')
          WHEN event_name = 'dt_storage - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'start_state')
          END AS value
      UNION ALL SELECT 'made_changes' AS key, CASE
          WHEN event_name = 'dt_inspector - edit_html' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'made_changes')
          END AS value
      UNION ALL SELECT 'time_open' AS key, CASE
          WHEN event_name = 'dt_inspector - edit_html' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'time_open')
          END AS value
      UNION ALL SELECT 'oldpanel' AS key, CASE
          WHEN event_name = 'dt_inspector - sidepanel_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'oldpanel')
          WHEN event_name = 'dt_netmonitor - sidepanel_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'oldpanel')
          END AS value
      UNION ALL SELECT 'newpanel' AS key, CASE
          WHEN event_name = 'dt_inspector - sidepanel_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'newpanel')
          WHEN event_name = 'dt_netmonitor - sidepanel_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'newpanel')
          END AS value
      UNION ALL SELECT 'exceptions' AS key, CASE
          WHEN event_name = 'dt_jsdebugger - pause_on_exceptions' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'exceptions')
          END AS value
      UNION ALL SELECT 'caught_exceptio' AS key, CASE
          WHEN event_name = 'dt_jsdebugger - pause_on_exceptions' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'caught_exceptio')
          END AS value
      UNION ALL SELECT 'lib_stacks' AS key, CASE
          WHEN event_name = 'dt_jsdebugger - pause' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'lib_stacks')
          END AS value
      UNION ALL SELECT 'mode' AS key, CASE
          WHEN event_name = 'dt_netmonitor - throttle_changed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'mode')
          END AS value
      UNION ALL SELECT 'panel_name' AS key, CASE
          WHEN event_name = 'dt_other - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'panel_name')
          END AS value
      UNION ALL SELECT 'connection_type' AS key, CASE
          WHEN event_name = 'dt_adbg - device_added' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'connection_type')
          WHEN event_name = 'dt_adbg - device_removed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'connection_type')
          WHEN event_name = 'dt_adbg - runtime_added' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'connection_type')
          WHEN event_name = 'dt_adbg - runtime_connected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'connection_type')
          WHEN event_name = 'dt_adbg - runtime_disconnected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'connection_type')
          WHEN event_name = 'dt_adbg - runtime_removed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'connection_type')
          END AS value
      UNION ALL SELECT 'device_name' AS key, CASE
          WHEN event_name = 'dt_adbg - device_added' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'device_name')
          WHEN event_name = 'dt_adbg - device_removed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'device_name')
          WHEN event_name = 'dt_adbg - runtime_added' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'device_name')
          WHEN event_name = 'dt_adbg - runtime_connected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'device_name')
          WHEN event_name = 'dt_adbg - runtime_disconnected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'device_name')
          WHEN event_name = 'dt_adbg - runtime_removed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'device_name')
          END AS value
      UNION ALL SELECT 'runtime_type' AS key, CASE
          WHEN event_name = 'dt_adbg - inspect' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_type')
          END AS value
      UNION ALL SELECT 'target_type' AS key, CASE
          WHEN event_name = 'dt_adbg - inspect' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'target_type')
          END AS value
      UNION ALL SELECT 'runtime_id' AS key, CASE
          WHEN event_name = 'dt_adbg - runtime_added' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_id')
          WHEN event_name = 'dt_adbg - runtime_connected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_id')
          WHEN event_name = 'dt_adbg - runtime_disconnected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_id')
          WHEN event_name = 'dt_adbg - runtime_removed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_id')
          WHEN event_name = 'dt_adbg - show_profiler' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_id')
          WHEN event_name = 'dt_adbg - update_conn_prompt' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_id')
          END AS value
      UNION ALL SELECT 'runtime_name' AS key, CASE
          WHEN event_name = 'dt_adbg - runtime_added' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_name')
          WHEN event_name = 'dt_adbg - runtime_connected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_name')
          WHEN event_name = 'dt_adbg - runtime_disconnected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_name')
          WHEN event_name = 'dt_adbg - runtime_removed' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_name')
          END AS value
      UNION ALL SELECT 'runtime_os' AS key, CASE
          WHEN event_name = 'dt_adbg - runtime_connected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_os')
          END AS value
      UNION ALL SELECT 'runtime_version' AS key, CASE
          WHEN event_name = 'dt_adbg - runtime_connected' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'runtime_version')
          END AS value
      UNION ALL SELECT 'page_type' AS key, CASE
          WHEN event_name = 'dt_adbg - select_page' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'page_type')
          END AS value
      UNION ALL SELECT 'prompt_enabled' AS key, CASE
          WHEN event_name = 'dt_adbg - update_conn_prompt' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'prompt_enabled')
          END AS value
  ) WHERE VALUE IS NOT NULL) AS event_props_2,
  (SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
  SELECT 'host' AS key, CASE
          WHEN event_name = 'dt - open' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt - close' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_webconsole - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_webconsole - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_inspector - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_inspector - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_jsdebugger - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_jsdebugger - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_styleeditor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_styleeditor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_netmonitor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_netmonitor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_storage - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_storage - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_other - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_other - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_responsive_design - activate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_responsive_design - deactivate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_split_console - activate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          WHEN event_name = 'dt_split_console - deactivate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'host')
          END AS value
      UNION ALL SELECT 'width' AS key, CASE
          WHEN event_name = 'dt - open' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt - close' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_webconsole - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_webconsole - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_inspector - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_inspector - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_jsdebugger - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_jsdebugger - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_styleeditor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_styleeditor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_netmonitor - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_netmonitor - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_storage - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_storage - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_other - enter' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_other - exit' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_responsive_design - activate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_responsive_design - deactivate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_split_console - activate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_split_console - deactivate' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_adbg - close_adbg' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          WHEN event_name = 'dt_adbg - open_adbg' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'width')
          END AS value
)) AS user_props
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
