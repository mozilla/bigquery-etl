-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `{{ target_view }}`
AS
{% for dataset in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
SELECT {{ dataset.select_expression }}
FROM `{{ project_id }}.{{ dataset.dataset }}.{{ dataset.table }}`
{% endfor %}
