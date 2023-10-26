{% for (dataset, channel) in datasets -%}
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
FROM `{{ project_id }}.{{ dataset }}_derived.event_monitoring_live_v1`
UNION ALL
{% endfor %}
SELECT 
  TIMESTAMP(submission_date) AS window_start,
  TIMESTAMP(DATE_ADD(submission_date, INTERVAL 1 DAY)) AS window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  normalized_app_name,
  normalized_channel,
  version,
  total_events
FROM `{{ project_id }}.{{ app_name }}.event_monitoring_aggregates_v1`

