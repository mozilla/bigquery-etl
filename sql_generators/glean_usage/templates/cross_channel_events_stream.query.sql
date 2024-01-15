-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ target_view }}`
AS
{% for (dataset, channel) in datasets %}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT
  "{{ dataset }}" AS normalized_app_id,
  e.*
  REPLACE(
    {% if app_name == "fenix" -%}
    mozfun.norm.fenix_app_info("{{ dataset }}", client_info.app_build).channel AS normalized_channel
    {% elif datasets|length > 1 -%}
    "{{ channel }}" AS normalized_channel
    {% endif -%}
  ),
FROM `{{ project_id }}.{{ dataset }}_derived.events_stream` AS e
{% endfor %}
