CREATE OR REPLACE VIEW
    `moz-fx-data-shared-prod.telemetry.desktop_savant_events_v1` AS
WITH base_events AS (

SELECT
  *,
  timestamp AS submission_timestamp,
  event_string_value AS event_value
FROM
  `moz-fx-data-shared-prod.telemetry.events`

), all_events AS (
SELECT
    submission_date,
    submission_timestamp,
    client_id AS device_id,
    `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'session_id') AS session_id_offset,
    CONCAT(event_category, '.', event_method) AS event_type,
    CASE
        WHEN (event_category IN ('meta') ) AND (event_method IN ('session_split') ) THEN 'Meta - session split v3' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('follow_bookmark') ) AND (event_object IN ('open') ) THEN 'SAVANT - open bookmark' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('bookmark') ) AND (event_object IN ('save') ) THEN 'SAVANT - save bookmark' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('bookmark') ) AND (event_object IN ('remove') ) THEN 'SAVANT - remove bookmark' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('hamburger_menu') ) AND (event_object IN ('open') ) THEN 'SAVANT - open hamburger menu' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('library_menu') ) AND (event_object IN ('open') ) THEN 'SAVANT - open library menu' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('dotdotdot_menu') ) AND (event_object IN ('open') ) THEN 'SAVANT - open dotdotdot menu' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('readermode') ) AND (event_object IN ('on') ) THEN 'SAVANT - on readermode' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('readermode') ) AND (event_object IN ('off') ) THEN 'SAVANT - off readermode' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('tab') ) AND (event_object IN ('open') ) THEN 'SAVANT - open tab' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('tab') ) AND (event_object IN ('close') ) THEN 'SAVANT - close tab' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('tab') ) AND (event_object IN ('select') ) THEN 'SAVANT - select tab' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('follow_urlbar_link') ) AND (event_object IN ('bookmark') ) THEN 'SAVANT - follow bookmark url' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('follow_urlbar_link') ) AND (event_object IN ('history') ) THEN 'SAVANT - follow history url' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('search') ) THEN 'SAVANT - made search' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('addon') ) AND (event_object IN ('enable') ) THEN 'SAVANT - enable addon' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('addon') ) AND (event_object IN ('disable') ) THEN 'SAVANT - disable addon' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('addon') ) AND (event_object IN ('install_start') ) THEN 'SAVANT - install addon start' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('addon') ) AND (event_object IN ('install_finish') ) THEN 'SAVANT - install addon finish' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('addon') ) AND (event_object IN ('remove_start') ) THEN 'SAVANT - remove addon start' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('addon') ) AND (event_object IN ('remove_finish') ) THEN 'SAVANT - remove addon finish' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('login_form') ) AND (event_object IN ('load') ) THEN 'SAVANT - load login form' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('login_form') ) AND (event_object IN ('submit') ) THEN 'SAVANT - submit login form' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('pwmgr_use') ) AND (event_object IN ('use') ) THEN 'SAVANT - use pw manager' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('pwmgr') ) AND (event_object IN ('ask') ) THEN 'SAVANT - asked add pw' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('pwmgr') ) AND (event_object IN ('save') ) THEN 'SAVANT - saved pw' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('pwmgr') ) AND (event_object IN ('update') ) THEN 'SAVANT - updated pw' 
        WHEN (event_category IN ('savant') ) AND (event_method IN ('end_study') ) THEN 'SAVANT - study ended'
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
    created
FROM
    base_events
WHERE doc_type IN ('main') AND (`moz-fx-data-derived-datasets.udf.get_key`(experiments, 'pref-flip-savant-1457226-de-existing-users') IS NOT NULL OR `moz-fx-data-derived-datasets.udf.get_key`(experiments, 'pref-flip-savant-1457226-de-new-users') IS NOT NULL OR `moz-fx-data-derived-datasets.udf.get_key`(experiments, 'pref-flip-savant-1457226-en-existing-users') IS NOT NULL OR `moz-fx-data-derived-datasets.udf.get_key`(experiments, 'pref-flip-savant-1457226-en-new-users') IS NOT NULL)
), all_events_with_insert_ids AS (
SELECT
  * EXCEPT (event_category, created),
  CONCAT(device_id, "-", CAST(created AS STRING), "-", event_name, "-", CAST(timestamp AS STRING), "-", event_category, "-", event_method, "-", event_object) AS insert_id
FROM
  all_events
WHERE
  event_name IS NOT NULL
), extra_props AS (
SELECT
  * EXCEPT (event_map_values, event_object, event_value, event_method),
  (SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
      SELECT 'subsession_length' AS key, CASE
          WHEN event_name = 'Meta - session split v3' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'subsession_length')
          END AS value
      UNION ALL SELECT 'active_ticks' AS key, CASE
          WHEN event_name = 'Meta - session split v3' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'active_ticks')
          END AS value
      UNION ALL SELECT 'uri_count' AS key, CASE
          WHEN event_name = 'Meta - session split v3' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'uri_count')
          END AS value
      UNION ALL SELECT 'search_count' AS key, CASE
          WHEN event_name = 'Meta - session split v3' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'search_count')
          END AS value
      UNION ALL SELECT 'reason' AS key, CASE
          WHEN event_name = 'Meta - session split v3' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'reason')
          WHEN event_name = 'SAVANT - study ended' THEN event_object
          END AS value
      UNION ALL SELECT 'meta-cat-1' AS key, CASE
          WHEN event_name = 'SAVANT - open bookmark' THEN 'navigate'
          WHEN event_name = 'SAVANT - save bookmark' THEN 'feature'
          WHEN event_name = 'SAVANT - remove bookmark' THEN 'feature'
          WHEN event_name = 'SAVANT - open hamburger menu' THEN 'feature'
          WHEN event_name = 'SAVANT - open library menu' THEN 'feature'
          WHEN event_name = 'SAVANT - open dotdotdot menu' THEN 'feature'
          WHEN event_name = 'SAVANT - on readermode' THEN 'feature'
          WHEN event_name = 'SAVANT - off readermode' THEN 'feature'
          WHEN event_name = 'SAVANT - open tab' THEN 'frame'
          WHEN event_name = 'SAVANT - close tab' THEN 'frame'
          WHEN event_name = 'SAVANT - select tab' THEN 'frame'
          WHEN event_name = 'SAVANT - follow bookmark url' THEN 'navigate'
          WHEN event_name = 'SAVANT - follow history url' THEN 'navigate'
          WHEN event_name = 'SAVANT - made search' THEN 'navigate'
          WHEN event_name = 'SAVANT - enable addon' THEN 'feature'
          WHEN event_name = 'SAVANT - disable addon' THEN 'feature'
          WHEN event_name = 'SAVANT - install addon start' THEN 'feature'
          WHEN event_name = 'SAVANT - install addon finish' THEN 'feature'
          WHEN event_name = 'SAVANT - remove addon start' THEN 'feature'
          WHEN event_name = 'SAVANT - remove addon finish' THEN 'feature'
          WHEN event_name = 'SAVANT - load login form' THEN 'encounter'
          WHEN event_name = 'SAVANT - submit login form' THEN 'encounter'
          WHEN event_name = 'SAVANT - use pw manager' THEN 'feature'
          WHEN event_name = 'SAVANT - asked add pw' THEN 'popup'
          WHEN event_name = 'SAVANT - saved pw' THEN 'feature'
          WHEN event_name = 'SAVANT - updated pw' THEN 'feature'
          WHEN event_name = 'SAVANT - study ended' THEN 'shield'
          END AS value
      UNION ALL SELECT 'meta-cat-2' AS key, CASE
          WHEN event_name = 'SAVANT - open bookmark' THEN 'bookmark'
          WHEN event_name = 'SAVANT - save bookmark' THEN 'bookmark'
          WHEN event_name = 'SAVANT - remove bookmark' THEN 'bookmark'
          WHEN event_name = 'SAVANT - on readermode' THEN 'readermode'
          WHEN event_name = 'SAVANT - off readermode' THEN 'readermode'
          WHEN event_name = 'SAVANT - open tab' THEN 'tab'
          WHEN event_name = 'SAVANT - close tab' THEN 'tab'
          WHEN event_name = 'SAVANT - select tab' THEN 'tab'
          WHEN event_name = 'SAVANT - follow bookmark url' THEN 'url_bar'
          WHEN event_name = 'SAVANT - follow history url' THEN 'url_bar'
          WHEN event_name = 'SAVANT - enable addon' THEN 'addon'
          WHEN event_name = 'SAVANT - disable addon' THEN 'addon'
          WHEN event_name = 'SAVANT - install addon start' THEN 'addon'
          WHEN event_name = 'SAVANT - install addon finish' THEN 'addon'
          WHEN event_name = 'SAVANT - remove addon start' THEN 'addon'
          WHEN event_name = 'SAVANT - remove addon finish' THEN 'addon'
          WHEN event_name = 'SAVANT - load login form' THEN 'loginform'
          WHEN event_name = 'SAVANT - submit login form' THEN 'loginform'
          WHEN event_name = 'SAVANT - use pw manager' THEN 'pwmgr'
          WHEN event_name = 'SAVANT - asked add pw' THEN 'pwmgr'
          WHEN event_name = 'SAVANT - saved pw' THEN 'pwmgr'
          WHEN event_name = 'SAVANT - updated pw' THEN 'pwmgr'
          END AS value
      UNION ALL SELECT 'entrypoint' AS key, CASE
          WHEN event_name = 'SAVANT - made search' THEN event_object
          END AS value
      UNION ALL SELECT 'type' AS key, CASE
          WHEN event_name = 'SAVANT - made search' THEN event_value
          END AS value
      UNION ALL SELECT 'engine' AS key, CASE
          WHEN event_name = 'SAVANT - made search' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'engine')
          END AS value
      UNION ALL SELECT 'addonid' AS key, CASE
          WHEN event_name = 'SAVANT - enable addon' THEN event_value
          WHEN event_name = 'SAVANT - disable addon' THEN event_value
          WHEN event_name = 'SAVANT - install addon start' THEN event_value
          WHEN event_name = 'SAVANT - install addon finish' THEN event_value
          WHEN event_name = 'SAVANT - remove addon start' THEN event_value
          WHEN event_name = 'SAVANT - remove addon finish' THEN event_value
          END AS value
      UNION ALL SELECT 'can-record-submit' AS key, CASE
          WHEN event_name = 'SAVANT - load login form' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'canRecordSubmit')
          END AS value
      UNION ALL SELECT 'flow-id' AS key, CASE
          WHEN event_name = 'SAVANT - load login form' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'flow_id')
          WHEN event_name = 'SAVANT - submit login form' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'flow_id')
          WHEN event_name = 'SAVANT - use pw manager' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'flow_id')
          WHEN event_name = 'SAVANT - asked add pw' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'flow_id')
          WHEN event_name = 'SAVANT - saved pw' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'flow_id')
          WHEN event_name = 'SAVANT - updated pw' THEN `moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'flow_id')
          END AS value
      UNION ALL SELECT 'add-type' AS key, CASE
          WHEN event_name = 'SAVANT - asked add pw' THEN event_value
          END AS value
  ) WHERE VALUE IS NOT NULL) AS event_props_2,
  ARRAY<STRING>[] AS user_props
FROM
  all_events_with_insert_ids
)

SELECT
  * EXCEPT (event_props_1, event_props_2, user_props),
  CONCAT('{', ARRAY_TO_STRING((
   SELECT ARRAY_AGG(DISTINCT e) FROM UNNEST(ARRAY_CONCAT(event_props_1, event_props_2)) AS e
  ), ","), '}') AS event_properties,
  CONCAT('{', ARRAY_TO_STRING(user_props, ","), '}') AS user_properties
FROM extra_props
