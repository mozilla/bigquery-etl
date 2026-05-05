{% set fivetran_gsc_datasets = [
    {'id': 'moz-fx-data-bq-fivetran.google_search_console_addons', 'query_column': 'keyword'},
    {'id': 'moz-fx-data-bq-fivetran.google_search_console_blog', 'query_column': 'keyword'},
    {'id': 'moz-fx-data-bq-fivetran.google_search_console_mdn', 'query_column': 'query'},
    {'id': 'moz-fx-data-bq-fivetran.google_search_console_pocket', 'query_column': 'keyword'},
    {'id': 'moz-fx-data-bq-fivetran.google_search_console_support', 'query_column': 'keyword'},
    {'id': 'moz-fx-data-bq-fivetran.google_search_console_www', 'query_column': 'keyword'},
] %}
WITH keyword_page_report_union AS (
  {% for fivetran_gsc_dataset in fivetran_gsc_datasets %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
      `date`,
      site,
      page,
      `{{ fivetran_gsc_dataset['query_column'] }}` AS query,
      search_type,
      country,
      device,
      impressions,
      clicks,
      position
    FROM
      `{{ fivetran_gsc_dataset['id'] }}.keyword_page_report`
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
  query,
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
