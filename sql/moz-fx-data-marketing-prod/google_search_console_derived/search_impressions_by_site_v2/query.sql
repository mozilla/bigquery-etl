WITH searchdata_site_impression_union AS (
  SELECT
    data_date,
    site_url,
    query,
    is_anonymized_query,
    search_type,
    country,
    device,
    impressions,
    clicks,
    sum_top_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_addons.searchdata_site_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    query,
    is_anonymized_query,
    search_type,
    country,
    device,
    impressions,
    clicks,
    sum_top_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_blog.searchdata_site_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    query,
    is_anonymized_query,
    search_type,
    country,
    device,
    impressions,
    clicks,
    sum_top_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_getpocket.searchdata_site_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    query,
    is_anonymized_query,
    search_type,
    country,
    device,
    impressions,
    clicks,
    sum_top_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_support.searchdata_site_impression`
  UNION ALL
  SELECT
    data_date,
    site_url,
    query,
    is_anonymized_query,
    search_type,
    country,
    device,
    impressions,
    clicks,
    sum_top_position
  FROM
    `moz-fx-data-marketing-prod.searchconsole_www.searchdata_site_impression`
)
SELECT
  data_date AS `date`,
  site_url,
  REGEXP_EXTRACT(site_url, r'^(?:https?://|sc-domain:)([^/]+)') AS site_domain_name,
  query,
  is_anonymized_query AS is_anonymized,
  INITCAP(REPLACE(search_type, '_', ' ')) AS search_type,
  UPPER(country) AS country_code,
  INITCAP(device) AS device_type,
  impressions,
  clicks,
  ((sum_top_position / impressions) + 1) AS average_top_position
FROM
  searchdata_site_impression_union
WHERE
  data_date = @date
