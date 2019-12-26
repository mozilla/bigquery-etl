CREATE OR REPLACE VIEW
    `moz-fx-data-shared-prod.telemetry.fire_tv_events_v1` AS
WITH base_events AS (

SELECT
  *,
  event.f0_ AS timestamp,
  event.f0_ AS event_timestamp,
  event.f1_ AS event_category,
  event.f2_ AS event_method,
  event.f3_ AS event_object,
  event.f4_ AS event_value,
  event.f5_ AS event_map_values,
  metadata.uri.app_version,
  osversion AS os_version,
  metadata.geo.country,
  metadata.geo.city,
  metadata.uri.app_name
FROM
  `moz-fx-data-shared-prod.telemetry.mobile_event`
  CROSS JOIN UNNEST(events) AS event

), all_events AS (
SELECT
    submission_timestamp,
    client_id AS device_id,
    (created + COALESCE(SAFE_CAST(`moz-fx-data-derived-datasets.udf.get_key`(event_map_values, 'session_id') AS INT64), 0)) AS session_id,
    CASE
        WHEN (event_category IN ('action') ) AND (event_method IN ('page') ) AND (event_object IN ('browser') ) AND (event_value IN ('back') ) THEN 'Firefox for Fire TV - app - back' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('foreground') ) AND (event_object IN ('app') ) THEN 'Firefox for Fire TV - app - foreground' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('intent_url') ) AND (event_object IN ('app') ) THEN 'Firefox for Fire TV - app - intent_url' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('pin_page') ) AND (event_value IN ('on') ) THEN 'Firefox for Fire TV - app - pin_page' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('pin_page') ) AND (event_value IN ('off') ) THEN 'Firefox for Fire TV - app - unpin_page' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('view_intent') ) AND (event_object IN ('app') ) THEN 'Firefox for Fire TV - app - view_intent_open' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('home_tile') ) AND (event_value IN ('bundled') ) THEN 'Firefox for Fire TV - bundled_tile - remove' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('home_tile') ) AND (event_value IN ('bundled') ) THEN 'Firefox for Fire TV - bundled_tile - click' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('home_tile') ) AND (event_value IN ('custom') ) THEN 'Firefox for Fire TV - custom_tile - click' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('remove') ) AND (event_object IN ('home_tile') ) AND (event_value IN ('custom') ) THEN 'Firefox for Fire TV - custom_tile - remove' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click_or_voice') ) AND (event_object IN ('media_session') ) AND (event_value IN ('seek') ) THEN 'Firefox for Fire TV - media - seek' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click_or_voice') ) AND (event_object IN ('media_session') ) AND (event_value IN ('next') ) THEN 'Firefox for Fire TV - media - next' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click_or_voice') ) AND (event_object IN ('media_session') ) AND (event_value IN ('prev') ) THEN 'Firefox for Fire TV - media - previous' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('back') ) THEN 'Firefox for Fire TV - menu - back' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('exit') ) THEN 'Firefox for Fire TV - menu - exit' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('forward') ) THEN 'Firefox for Fire TV - menu - forward' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('user_hide') ) AND (event_object IN ('menu') ) THEN 'Firefox for Fire TV - menu - hide' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('home') ) THEN 'Firefox for Fire TV - menu - home' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('pocket_video_tile') ) THEN 'Firefox for Fire TV - menu - pocket_video_tile' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('refresh') ) THEN 'Firefox for Fire TV - menu - refresh' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('menu') ) AND (event_value IN ('settings') ) THEN 'Firefox for Fire TV - menu - settings' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('user_show') ) AND (event_object IN ('menu') ) THEN 'Firefox for Fire TV - menu - show' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('type_query') ) AND (event_object IN ('search_bar') ) THEN 'Firefox for Fire TV - search_bar - type_query' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('type_url') ) AND (event_object IN ('search_bar') ) THEN 'Firefox for Fire TV - search_bar - type_url' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('setting') ) AND (event_value IN ('clear_data') ) THEN 'Firefox for Fire TV - settings - clear_data' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('desktop_mode') ) THEN 'Firefox for Fire TV - settings - desktop_mode' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('change') ) AND (event_object IN ('turbo_mode') ) THEN 'Firefox for Fire TV - settings - turbo_mode' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('home_tile') ) THEN 'Firefox for Fire TV - tile - click' 
        WHEN (event_category IN ('action') ) AND (event_method IN ('click') ) AND (event_object IN ('home_tile') ) AND (event_value IN ('youtube_tile') ) THEN 'Firefox for Fire TV - youtube_tile - click' 
        WHEN (event_category IN ('pocket') ) AND (event_method IN ('impression') ) AND (event_object IN ('video_id') ) THEN 'Firefox for Fire TV - pocket_video - impression' 
        WHEN (event_category IN ('pocket') ) AND (event_method IN ('click') ) AND (event_object IN ('video_id') ) THEN 'Firefox for Fire TV - pocket_video - click'
    END AS event_name,
    event_timestamp AS timestamp,
    (event_timestamp + created) AS time,
    app_version,
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
    settings
FROM
    base_events
WHERE app_name IN ('FirefoxForFireTV', 'OTHER') AND os IN ('Android')
), all_events_with_insert_ids AS (
SELECT
  * EXCEPT (event_category, created),
  CONCAT(device_id, "-", CAST(created AS STRING), "-", SUBSTR(event_name, 23), "-", CAST(timestamp AS STRING), "-", event_category, "-", event_method, "-", event_object) AS insert_id,
  event_name AS event_type
FROM
  all_events
WHERE
  event_name IS NOT NULL
), extra_props AS (
SELECT
  * EXCEPT (event_map_values, event_object, event_value, event_method, event_name),
  (SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
      SELECT 'settings_value' AS key, CASE
          WHEN event_name = 'Firefox for Fire TV - settings - desktop_mode' THEN event_value
          WHEN event_name = 'Firefox for Fire TV - settings - turbo_mode' THEN event_value
          END AS value
  ) WHERE VALUE IS NOT NULL) AS event_props_2,
  ARRAY_CONCAT(ARRAY<STRING>[],
    (SELECT ARRAY_AGG(
    CASE
        WHEN key='tracking_protection_enabled' THEN CONCAT('"', 'tracking_protection_enabled', '":', CAST(SAFE_CAST(value AS BOOLEAN) AS STRING))
        WHEN key='total_home_tile_count' THEN CONCAT('"', 'total_home_tile_count', '":"', CAST(value AS STRING), '"')
        WHEN key='custom_home_tile_count' THEN CONCAT('"', 'custom_home_tile_count', '":"', CAST(value AS STRING), '"')
        WHEN key='remote_control_name' THEN CONCAT('"', 'remote_control_name', '":"', CAST(value AS STRING), '"')
        WHEN key='app_id' THEN CONCAT('"', 'app_id', '":"', CAST(value AS STRING), '"')
    END
    IGNORE NULLS)
  FROM
    UNNEST(SETTINGS)
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
