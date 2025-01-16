CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.addons.addon_events_clients`
AS
SELECT
  submission_date,
  `timestamp`,
  COALESCE(
    udf.get_key(event_map_values, 'addonId'),
    udf.get_key(event_map_values, 'addon_id')
  ) AS addon_id,
  COALESCE(
    udf.get_key(event_map_values, 'addonVersion'),
    udf.get_key(event_map_values, 'addon_version')
  ) AS addon_version,
  -- TODO: why in some redash queries this is hashed_addon_id and in other value? What's the difference?
  event_string_value AS hashed_addon_id,
  client_id,
  sample_id,
  normalized_channel,
  country,
  locale,
  app_name,
  app_version,
  os AS os_name,
  os_version,
  event_timestamp,
  event_category,
  event_method AS method,
  event_object AS `object`,
  event_string_value AS `value`,
  event_map_values,
  event_process,
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  submission_date >= '2021-08-09'
  AND event_category IN (
    "addonsManager",
    "addonsSearchDetection"
  )
