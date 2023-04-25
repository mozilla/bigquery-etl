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
  {{ query.select_expression }}
FROM `{{ project_id }}.{{ query.dataset }}.{{ query.table }}`
{% endfor %}
