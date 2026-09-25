CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.blame_lines`
AS
{% for app_id in applications -%}
SELECT
  "{{ app_id }}" AS app_id,
  *
FROM
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_blame_lines_v1`
{%- if not loop.last %}
UNION ALL
{% endif -%}
{% endfor %}
