CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.signatures` AS
{% for app_id in applications -%}
SELECT
  *
FROM
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_signatures_v1`
{%- if not loop.last %}
UNION ALL
{% endif -%}
{% endfor %}
