WITH keyword_page_report_union AS (
  SELECT
    `date`,
    site,
    page,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_addons.keyword_page_report`
  UNION ALL
  SELECT
    `date`,
    site,
    page,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_blog.keyword_page_report`
  UNION ALL
  SELECT
    `date`,
    site,
    page,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_pocket.keyword_page_report`
  UNION ALL
  SELECT
    `date`,
    site,
    page,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_support.keyword_page_report`
  UNION ALL
  SELECT
    `date`,
    site,
    page,
    keyword,
    search_type,
    country,
    device,
    impressions,
    clicks,
    position
  FROM
    `moz-fx-data-bq-fivetran.google_search_console_www.keyword_page_report`
)
SELECT
  `date`,
  site AS site_url,
  REGEXP_EXTRACT(site, r'^(?:https?://|sc-domain:)([^/]+)') AS site_domain_name,
  page AS page_url,
  REGEXP_EXTRACT(page, r'^https?://([^/]+)') AS page_domain_name,
  REGEXP_EXTRACT(page, r'^https?://(?:[^/]+)([^\?#]*)') AS page_path,
  REGEXP_EXTRACT(page, r'^https?://(?:[^/]+)/*([^/\?#]*)') AS page_path_segment_1,
  keyword AS query,
  INITCAP(search_type) AS search_type,
  UPPER(country) AS country_code,
  INITCAP(device) AS device_type,
  CAST(impressions AS INTEGER) AS impressions,
  CAST(clicks AS INTEGER) AS clicks,
  position AS average_position
FROM
  keyword_page_report_union
WHERE
  `date` = @date
