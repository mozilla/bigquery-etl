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
  search_impressions.`date`,
  search_impressions.site_url,
  search_impressions.site_domain_name,
  search_impressions.query,
  mozfun.google_search_console.classify_query(
    search_impressions.query,
    search_impressions.search_type
  ) AS query_type,
  search_impressions.is_anonymized,
  search_impressions.search_type,
  search_impressions.user_country_code,
  COALESCE(user_country.name, search_impressions.user_country_code) AS user_country,
  search_impressions.device_type,
  search_impressions.impressions,
  search_impressions.clicks,
  search_impressions.average_top_position
FROM
  search_impressions_union AS search_impressions
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS user_country
  ON search_impressions.user_country_code = user_country.code_3
