CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.firefox_products_search_clients_engines_sources_daily`
AS
SELECT
  client_id,
  submission_date,
  source,
  "mobile" AS device,
  normalized_engine,
  normalized_app_name,
  os,
  country,
  sap AS searches,
  search_with_ads,
  ad_click,
  tagged_sap,
  tagged_follow_on,
  NULL AS profile_group_id
FROM
  `mozdata.search.mobile_search_clients_engines_sources_daily`
UNION ALL
SELECT
  client_id,
  submission_date,
  source,
  "desktop" AS device,
  normalized_engine,
  'Firefox Desktop' AS normalized_app_name,
  os,
  country,
  sap AS searches,
  search_with_ads,
  ad_click,
  tagged_sap,
  tagged_follow_on,
  profile_group_id
FROM
  `mozdata.search.search_clients_engines_sources_daily`
