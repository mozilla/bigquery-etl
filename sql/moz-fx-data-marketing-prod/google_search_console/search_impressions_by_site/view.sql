CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.google_search_console.search_impressions_by_site`
AS
WITH search_impressions_union AS (
  SELECT
    `date`,
    site_url,
    site_domain_name,
    query,
    FALSE AS is_anonymized,
    search_type,
    user_country_code,
    device_type,
    impressions,
    clicks,
    average_top_position
  FROM
    `moz-fx-data-marketing-prod.google_search_console_derived.search_impressions_by_site_v1`
  WHERE
    `date` < '2023-08-01'
  UNION ALL
  SELECT
    `date`,
    site_url,
    site_domain_name,
    query,
    is_anonymized,
    search_type,
    user_country_code,
    device_type,
    impressions,
    clicks,
    average_top_position
  FROM
    `moz-fx-data-marketing-prod.google_search_console_derived.search_impressions_by_site_v2`
  WHERE
    `date` >= '2023-08-01'
)
SELECT
  `date`,
  site_url,
  site_domain_name,
  query,
  mozfun.google_search_console.classify_query(query, search_type) AS query_type,
  is_anonymized,
  search_type,
  user_country_code,
  device_type,
  impressions,
  clicks,
  average_top_position
FROM
  search_impressions_union
