CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.google_search_console.search_impressions_by_page`
AS
SELECT
  `date`,
  site_url,
  site_domain_name,
  page_url,
  page_domain_name,
  page_path,
  page_path_segment_1,
  query,
  FALSE AS is_anonymized,
  CAST(NULL AS BOOLEAN) AS has_good_page_experience,
  search_type,
  CAST(NULL AS STRING) AS search_appearance,
  country_code,
  device_type,
  impressions,
  clicks,
  average_position
FROM
  `moz-fx-data-marketing-prod.google_search_console_derived.search_impressions_by_page_v1`
WHERE
  `date` < '2023-08-01'
UNION ALL
SELECT
  `date`,
  site_url,
  site_domain_name,
  page_url,
  page_domain_name,
  page_path,
  page_path_segment_1,
  query,
  is_anonymized,
  has_good_page_experience,
  search_type,
  search_appearance,
  country_code,
  device_type,
  impressions,
  clicks,
  average_position
FROM
  `moz-fx-data-marketing-prod.google_search_console_derived.search_impressions_by_page_v2`
WHERE
  `date` >= '2023-08-01'
