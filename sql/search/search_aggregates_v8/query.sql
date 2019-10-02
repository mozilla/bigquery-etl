SELECT
  submission_date,
  addon_version,
  app_version,
  country,
  distribution_id,
  engine,
  locale,
  search_cohort,
  source,
  default_search_engine,
  os,
  os_version,
  COUNT(*) as client_count,
  SUM(organic) as organic,
  SUM(tagged_sap) as tagged_sap,
  SUM(tagged_follow_on) as tagged_follow_on,
  SUM(sap) as sap,
  SUM(ad_click) as ad_click,
  SUM(search_with_ads) as search_with_ads,
  SUM(unknown) as unknown
FROM
  search_clients_daily_v8
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  addon_version,
  app_version,
  country,
  distribution_id,
  engine,
  locale,
  search_cohort,
  source,
  default_search_engine,
  os,
  os_version
