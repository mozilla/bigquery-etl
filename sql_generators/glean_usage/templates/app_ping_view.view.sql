-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `{{ target_view }}`
AS
{% for query in queries -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT
  "{{ query.dataset }}" AS normalized_app_id,
  {% if query.channel and query.app_name != "fenix" -%}
  "{{ query.channel }}" AS normalized_channel,
  {% elif query.app_name == "fenix" %}
  mozfun.norm.fenix_app_info("{{ query.dataset }}", client_info.app_build).channel AS normalized_channel,
  {% else %}
  normalized_channel,
  {% endif %}
  {{ query.select_expression }}
FROM `{{ project_id }}.{{ query.dataset }}.{{ query.table }}`
{% endfor %}
