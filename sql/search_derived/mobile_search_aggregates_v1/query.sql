SELECT
  submission_date,
  engine,
  normalized_engine,
  source,
  app_name,
  normalized_app_name,
  app_version,
  channel,
  country,
  locale,
  distribution_id,
  default_search_engine,
  os,
  os_version,
  COUNT(*) AS client_count,
  SUM(search_count) AS search_count
FROM
  mobile_search_clients_daily_v1
WHERE
  submission_date = @submission_date
  AND engine IS NOT NULL
GROUP BY
  submission_date,
  engine,
  normalized_engine,
  source,
  app_name,
  normalized_app_name,
  app_version,
  channel,
  country,
  locale,
  distribution_id,
  default_search_engine,
  os,
  os_version
