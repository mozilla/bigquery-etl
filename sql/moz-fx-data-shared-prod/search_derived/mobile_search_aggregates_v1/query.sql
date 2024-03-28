SELECT
  submission_date,
  engine,
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
  os_version_major,
  os_version_minor,
  COUNT(*) AS client_count,
  SUM(search_count) AS search_count,
  SUM(organic) AS organic,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(ad_click) AS ad_click,
  SUM(ad_click_organic) AS ad_click_organic,
  SUM(search_with_ads) AS search_with_ads,
  SUM(unknown) AS unknown,
  CAST(NULL AS string) normalized_engine,
FROM
  mobile_search_clients_daily_v1
WHERE
  submission_date = @submission_date
  AND engine IS NOT NULL
GROUP BY
  submission_date,
  engine,
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
  os_version_major,
  os_version_minor
