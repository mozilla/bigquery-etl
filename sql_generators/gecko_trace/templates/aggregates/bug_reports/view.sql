CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.bug_reports`
AS
{% for app_id in applications -%}
SELECT
  "{{ app_id }}" AS app_id,
  *
FROM
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_bug_reports_v1`
{%- if not loop.last %}
UNION ALL
{% endif -%}
{% endfor %}
