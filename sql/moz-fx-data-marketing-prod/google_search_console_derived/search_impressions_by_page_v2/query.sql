{% set gsc_export_dataset_ids = [
    'moz-fx-data-marketing-prod.searchconsole_addons',
    'moz-fx-data-marketing-prod.searchconsole_blog',
    'moz-fx-data-marketing-prod.searchconsole_getpocket',
    'moz-fx-data-marketing-prod.searchconsole_support',
    'moz-fx-data-marketing-prod.searchconsole_www',
] %}
{% set search_appearance_flag_columns = {
    'is_action': 'Action',
    'is_amp_blue_link': 'AMP non-rich result',
    'is_amp_image_result': 'AMP on image result',
    'is_amp_story': 'AMP story',
    'is_amp_top_stories': 'AMP top stories',
    'is_edu_q_and_a': 'Education Q&A',
    'is_events_details': 'Event details',
    'is_events_listing': 'Event listing',
    'is_job_details': 'Job details',
    'is_job_listing': 'Job listing',
    'is_learning_videos': 'Learning videos',
    'is_math_solvers': 'Math solvers',
    'is_merchant_listings': 'Merchant listings',
    'is_organic_shopping': 'Shopping',
    'is_practice_problems': 'Practice problems',
    'is_product_snippets': 'Product snippets',
    'is_recipe_feature': 'Recipe feature',
    'is_recipe_rich_snippet': 'Recipe rich snippet',
    'is_review_snippet': 'Review snippet',
    'is_search_appearance_android_app': 'Android app',
    'is_special_announcement': 'Special announcement',
    'is_subscribed_content': 'Subscribed content',
    'is_tpf_faq': 'FAQ rich result',
    'is_tpf_howto': 'How-to rich result',
    'is_tpf_qa': 'Q&A rich result',
    'is_translated_result': 'Translated result',
    'is_video': 'Video',
} %}
WITH searchdata_url_impression_union AS (
  {% for gsc_export_dataset_id in gsc_export_dataset_ids %}
    {% if not loop.first %}
      UNION ALL
    {% endif %}
    SELECT
      data_date,
      site_url,
      url,
      query,
      is_anonymized_query,
      is_anonymized_discover,
      is_page_experience,
      search_type,
      {{ search_appearance_flag_columns | join(',\n') }},
      country,
      device,
      impressions,
      clicks,
      sum_position
    FROM
      `{{ gsc_export_dataset_id }}.searchdata_url_impression`
  {% endfor %}
)
SELECT
  data_date AS `date`,
  site_url,
  REGEXP_EXTRACT(site_url, r'^(?:https?://|sc-domain:)([^/]+)') AS site_domain_name,
  url AS page_url,
  REGEXP_EXTRACT(url, r'^https?://([^/]+)') AS page_domain_name,
  REGEXP_EXTRACT(url, r'^https?://(?:[^/]+)([^\?#]*)') AS page_path,
  REGEXP_EXTRACT(url, r'^https?://(?:[^/]+)/*([^/\?#]*)') AS page_path_segment_1,
  query,
  (is_anonymized_query OR is_anonymized_discover) AS is_anonymized,
  is_page_experience AS has_good_page_experience,
  INITCAP(REPLACE(search_type, '_', ' ')) AS search_type,
  CASE
    {% for search_appearance_flag_column, search_appearance_label in search_appearance_flag_columns.items() %}
      WHEN {{ search_appearance_flag_column }}
        THEN '{{ search_appearance_label }}'
    {% endfor %}
    ELSE 'Normal result'
  END AS search_appearance,
  UPPER(country) AS country_code,
  INITCAP(device) AS device_type,
  impressions,
  clicks,
  ((sum_position / impressions) + 1) AS average_position
FROM
  searchdata_url_impression_union
WHERE
  {% if is_init() %}
    data_date < CURRENT_DATE()
  {% else %}
    data_date = @date
  {% endif %}
