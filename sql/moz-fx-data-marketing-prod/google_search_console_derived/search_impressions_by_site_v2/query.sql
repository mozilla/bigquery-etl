{% set gsc_export_dataset_ids = [
    'moz-fx-data-marketing-prod.searchconsole_addons',
    'moz-fx-data-marketing-prod.searchconsole_blog',
    'moz-fx-data-marketing-prod.searchconsole_getpocket',
    'moz-fx-data-marketing-prod.searchconsole_support',
    'moz-fx-data-marketing-prod.searchconsole_www',
] %}
WITH searchdata_site_impression_union AS (
  {% for gsc_export_dataset_id in gsc_export_dataset_ids %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
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
      `{{ gsc_export_dataset_id }}.searchdata_site_impression`
  {% endfor %}
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
  {% if is_init() %}
    data_date < CURRENT_DATE()
  {% else %}
    data_date = @date
  {% endif %}
