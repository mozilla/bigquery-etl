SELECT
  submission_date,
  normalized_channel,
  country,
  app_name,
  app_version,
  os_name,
  os_version,
  addon_id,
  addon_version,
  method,
  `object`,
  `value`,
  `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'from') AS from_etld,
  `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'to') AS to_etld,
  COUNT(*) AS event_count,
  COUNT(DISTINCT client_id) AS unique_clients_cnt,
FROM
  `moz-fx-data-shared-prod.tmp.addon_events_clients`
WHERE
  submission_date = @submission_date
  AND event_category = "addonsSearchDetection"
GROUP BY
  submission_date,
  normalized_channel,
  country,
  app_name,
  app_version,
  os_name,
  os_version,
  addon_id,
  addon_version,
  method,
  `object`,
  `value`,
  from_etld,
  to_etld
