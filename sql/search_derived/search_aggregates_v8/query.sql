SELECT
  submission_date,
  addon_version,
  app_version,
  country,
  distribution_id,
  engine,
  normalized_engine,
  locale,
  search_cohort,
  source,
  default_search_engine,
  default_private_search_engine,
  os,
  os_version,
  COUNT(*) AS client_count,
  SUM(organic) AS organic,
  SUM(tagged_sap) AS tagged_sap,
  SUM(tagged_follow_on) AS tagged_follow_on,
  SUM(sap) AS sap,
  SUM(ad_click) AS ad_click,
  SUM(search_with_ads) AS search_with_ads,
  SUM(unknown) AS unknown
FROM
  search_clients_daily_v8
WHERE
  submission_date = @submission_date
  AND engine IS NOT NULL
GROUP BY
  submission_date,
  addon_version,
  app_version,
  country,
  distribution_id,
  engine,
  normalized_engine,
  locale,
  search_cohort,
  source,
  default_search_engine,
  default_private_search_engine,
  os,
  os_version
