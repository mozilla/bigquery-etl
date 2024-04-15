{% set fivetran_gsc_dataset_ids = [
    'moz-fx-data-bq-fivetran.google_search_console_addons',
    'moz-fx-data-bq-fivetran.google_search_console_blog',
    'moz-fx-data-bq-fivetran.google_search_console_pocket',
    'moz-fx-data-bq-fivetran.google_search_console_support',
    'moz-fx-data-bq-fivetran.google_search_console_www',
] %}
WITH keyword_page_report_union AS (
  {% for fivetran_gsc_dataset_id in fivetran_gsc_dataset_ids %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
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
      `{{ fivetran_gsc_dataset_id }}.keyword_page_report`
  {% endfor %}
)
SELECT
  `date`,
  site AS site_url,
  mozfun.google_search_console.extract_url_domain_name(site) AS site_domain_name,
  page AS page_url,
  mozfun.google_search_console.extract_url_domain_name(page) AS page_domain_name,
  mozfun.google_search_console.extract_url_path(page) AS page_path,
  mozfun.google_search_console.extract_url_locale(page) AS localized_site_code,
  mozfun.google_search_console.extract_url_language_code(page) AS localized_site_language_code,
  mozfun.google_search_console.extract_url_country_code(page) AS localized_site_country_code,
  keyword AS query,
  INITCAP(search_type) AS search_type,
  UPPER(country) AS user_country_code,
  INITCAP(device) AS device_type,
  CAST(impressions AS INTEGER) AS impressions,
  CAST(clicks AS INTEGER) AS clicks,
  position AS average_position
FROM
  keyword_page_report_union
WHERE
  {% if is_init() %}
    `date` < CURRENT_DATE()
  {% else %}
    `date` = @date
  {% endif %}
