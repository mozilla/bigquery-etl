-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ target_view }}`
AS
{% for (dataset, channel) in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT
  {% if app_name == "fenix" -%}
  mozfun.norm.fenix_app_info("{{ dataset }}", client_info.app_build).channel AS normalized_channel,
  {% elif datasets|length > 1 -%}
  "{{ channel }}" AS normalized_channel,
  {% endif -%}
  normalized_app_name,
  window_start,
  window_end,
  event_category,
  event_name,
  event_extra_key,
  country,
  version,
  total_events
FROM
  `{{ project_id }}.{{ dataset }}_derived.event_monitoring_live_v1`
{% endfor %}