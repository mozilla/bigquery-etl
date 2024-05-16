CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.google_search_console.search_impressions_by_page`
AS
WITH search_impressions_union AS (
  SELECT
    `date`,
    site_url,
    site_domain_name,
    page_url,
    page_domain_name,
    page_path,
    localized_site_code,
    localized_site_language_code,
    localized_site_country_code,
    query,
    FALSE AS is_anonymized,
    CAST(NULL AS BOOLEAN) AS has_good_page_experience,
    search_type,
    CAST(NULL AS STRING) AS search_appearance,
    user_country_code,
    device_type,
    impressions,
    clicks,
    average_position
  FROM
    `moz-fx-data-marketing-prod.google_search_console_derived.search_impressions_by_page_v1`
  WHERE
    CASE
      WHEN site_domain_name IN (
          'addons.mozilla.org',
          'blog.mozilla.org',
          'getpocket.com',
          'support.mozilla.org',
          'www.mozilla.org'
        )
        THEN `date` < '2023-08-01'
      WHEN site_domain_name = 'developer.mozilla.org'
        THEN `date` < '2024-04-10'
      ELSE FALSE
    END
  UNION ALL
  SELECT
    `date`,
    site_url,
    site_domain_name,
    page_url,
    page_domain_name,
    page_path,
    localized_site_code,
    localized_site_language_code,
    localized_site_country_code,
    query,
    is_anonymized,
    has_good_page_experience,
    search_type,
    search_appearance,
    user_country_code,
    device_type,
    impressions,
    clicks,
    average_position
  FROM
    `moz-fx-data-marketing-prod.google_search_console_derived.search_impressions_by_page_v2`
  WHERE
    CASE
      WHEN site_domain_name IN (
          'addons.mozilla.org',
          'blog.mozilla.org',
          'getpocket.com',
          'support.mozilla.org',
          'www.mozilla.org'
        )
        THEN `date` >= '2023-08-01'
      ELSE TRUE
    END
)
SELECT
  search_impressions.date,
  search_impressions.site_url,
  search_impressions.site_domain_name,
  search_impressions.page_url,
  search_impressions.page_domain_name,
  search_impressions.page_path,
  search_impressions.localized_site_code,
  CONCAT(
    COALESCE(localized_site_language.name, search_impressions.localized_site_language_code),
    COALESCE(
      CONCAT(
        ' - ',
        COALESCE(localized_site_country.name, search_impressions.localized_site_country_code)
      ),
      ''
    )
  ) AS localized_site,
  search_impressions.localized_site_language_code,
  COALESCE(
    localized_site_language.name,
    search_impressions.localized_site_language_code
  ) AS localized_site_language,
  search_impressions.query,
  mozfun.google_search_console.classify_site_query(
    search_impressions.site_domain_name,
    search_impressions.query,
    search_impressions.search_type
  ) AS query_type,
  search_impressions.is_anonymized,
  search_impressions.has_good_page_experience,
  search_impressions.search_type,
  search_impressions.search_appearance,
  search_impressions.user_country_code,
  COALESCE(user_country.name, search_impressions.user_country_code) AS user_country,
  user_country.region_name AS user_region,
  user_country.subregion_name AS user_subregion,
  search_impressions.device_type,
  search_impressions.impressions,
  search_impressions.clicks,
  search_impressions.average_position
FROM
  search_impressions_union AS search_impressions
LEFT JOIN
  `moz-fx-data-shared-prod.static.language_codes_v1` AS localized_site_language
  ON search_impressions.localized_site_language_code = localized_site_language.code_2
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS localized_site_country
  ON search_impressions.localized_site_country_code = localized_site_country.code
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS user_country
  ON search_impressions.user_country_code = user_country.code_3
