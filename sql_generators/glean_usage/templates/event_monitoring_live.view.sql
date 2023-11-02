CREATE OR REPLACE VIEW `{{ project_id }}.{{ target_view }}` AS
{% for app in apps %}
{% set outer_loop = loop -%}
{% for dataset in app -%}
{% if dataset['bq_dataset_family'] in prod_datasets %}
SELECT
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  total_events
FROM 
  `{{ project_id }}.{{ dataset['bq_dataset_family'] }}_derived.event_monitoring_live_v1`
WHERE 
  submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
UNION ALL
{% endif %}
{% endfor %}
{% endfor %}
SELECT 
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  total_events
FROM 
  `{{ project_id }}.{{ target_table }}`
WHERE
  submission_date <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
