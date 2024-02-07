WITH keyword_site_report_by_site_union AS (
  SELECT
    `date`,
    site,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_addons.keyword_site_report_by_site`
  UNION ALL
  SELECT
    `date`,
    site,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_blog.keyword_site_report_by_site`
  UNION ALL
  SELECT
    `date`,
    site,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_pocket.keyword_site_report_by_site`
  UNION ALL
  SELECT
    `date`,
    site,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_support.keyword_site_report_by_site`
  UNION ALL
  SELECT
    `date`,
    site,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_www.keyword_site_report_by_site`
)
SELECT
  `date`,
  site AS site_url,
  REGEXP_EXTRACT(site, r'^(?:https?://|sc-domain:)([^/]+)') AS site_domain_name,
  keyword AS query,
  INITCAP(search_type) AS search_type,
  UPPER(country) AS country_code,
  INITCAP(device) AS device_type,
  CAST(impressions AS INTEGER) AS impressions,
  CAST(clicks AS INTEGER) AS clicks,
  position AS average_top_position
FROM
  keyword_site_report_by_site_union
WHERE
  `date` = @date
