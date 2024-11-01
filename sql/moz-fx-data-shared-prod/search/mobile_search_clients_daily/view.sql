CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search.mobile_search_clients_daily`
AS
SELECT
  submission_date,
  client_id,
  engine,
  source,
  app_name,
  search_count,
  organic,
  tagged_sap,
  tagged_follow_on,
  ad_click,
  search_with_ads,
  unknown,
  country,
  locale,
  app_version,
  channel,
  os,
  os_version,
  default_search_engine,
  default_search_engine_submission_url,
  distribution_id,
  profile_creation_date,
  profile_age_in_days,
  sample_id,
  experiments,
  total_uri_count,
  ad_click_organic,
  CAST(search_with_ads_organic AS INT64) AS search_with_ads_organic,
  os_version_major,
  os_version_minor,
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) AS normalized_engine,
  `mozfun.mobile_search.normalize_app_name`(
    app_name,
    os
  ).normalized_app_name AS normalized_app_name,
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info,
  search_count AS sap,
  `mozfun.mobile_search.normalize_app_name`(
    app_name,
    os
  ).normalized_app_name AS normalized_app_name_os
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_historical_pre202408`
WHERE
  submission_date <= '2024-07-31'
UNION ALL
SELECT
  submission_date,
  client_id,
  engine,
  source,
  app_name,
  search_count,
  organic,
  tagged_sap,
  tagged_follow_on,
  ad_click,
  search_with_ads,
  unknown,
  country,
  locale,
  app_version,
  channel,
  os,
  os_version,
  default_search_engine,
  default_search_engine_submission_url,
  distribution_id,
  profile_creation_date,
  profile_age_in_days,
  sample_id,
  experiments,
  total_uri_count,
  ad_click_organic,
  CAST(search_with_ads_organic AS INT64) AS search_with_ads_organic,
  os_version_major,
  os_version_minor,
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(engine) AS normalized_engine,
  `mozfun.mobile_search.normalize_app_name`(
    app_name,
    os
  ).normalized_app_name AS normalized_app_name,
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info,
  search_count AS sap,
  `mozfun.mobile_search.normalize_app_name`(
    app_name,
    os
  ).normalized_app_name AS normalized_app_name_os
FROM
  `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v2`
WHERE
  submission_date > '2024-07-31'
