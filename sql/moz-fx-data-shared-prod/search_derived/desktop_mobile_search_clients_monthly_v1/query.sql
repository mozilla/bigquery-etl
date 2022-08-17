-- Query for search_derived.desktop_mobile_monthly_search_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT
  client_id,
  DATE_TRUNC(submission_date, MONTH) AS month,
  "mobile" AS device,
  normalized_engine,
  normalized_app_name,
  os,
  country,
  COUNT(DISTINCT submission_date) AS days_of_use,
  COALESCE(SUM(sap), 0) AS searches,
  COALESCE(SUM(search_with_ads), 0) AS search_with_ads,
  COALESCE(SUM(ad_click), 0) AS ad_click,
  COALESCE(SUM(tagged_follow_on), 0) AS tagged_follow_on,
  COALESCE(SUM(tagged_sap), 0) AS tagged_sap,
FROM
  search.mobile_search_clients_engines_sources_daily
WHERE
  submission_date
  BETWEEN date_trunc(date_sub(@submission_date, INTERVAL 1 month), month)
  AND last_day(date_sub(@submission_date, INTERVAL 1 month), month)
GROUP BY
  client_id,
  month,
  device,
  normalized_engine,
  normalized_app_name,
  os,
  country
UNION ALL
SELECT
  client_id,
  DATE_TRUNC(submission_date, MONTH) AS month,
  "desktop" AS device,
  normalized_engine,
  'Firefox Desktop' AS normalized_app_name,
  os,
  country,
  count(DISTINCT submission_date) AS days_of_use,
  COALESCE(SUM(sap), 0) AS searches,
  COALESCE(SUM(search_with_ads), 0) AS search_with_ads,
  COALESCE(SUM(ad_click), 0) AS ad_click,
  COALESCE(SUM(tagged_follow_on), 0) AS tagged_follow_on,
  COALESCE(SUM(tagged_sap), 0) AS tagged_sap,
FROM
  search.search_clients_engines_sources_daily
WHERE
  submission_date
  BETWEEN date_trunc(date_sub(@submission_date, INTERVAL 1 month), month)
  AND last_day(date_sub(@submission_date, INTERVAL 1 month), month)
GROUP BY
  client_id,
  month,
  device,
  normalized_engine,
  normalized_app_name,
  os,
  country
