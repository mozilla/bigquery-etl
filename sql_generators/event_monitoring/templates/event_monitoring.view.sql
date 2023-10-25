{% for app in apps %}
SELECT
  "{{ app['app_id'] }}" AS app_id,
  "{{ app['app_name'] }}" AS app_name,
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  normalized_channel,
  version,
  total_events
FROM
  `{{ project_id }}.{{ app['app_id'] }}_derived.event_monitoring_live`
{% if not loop.last %} UNION ALL{% endif %}
{% endfor %}