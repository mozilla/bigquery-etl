CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.trace_events`
AS
{% for app_id in applications -%}
SELECT
  "{{ app_id }}" AS app_id,
  t.stable_trace_id,
  te.trace_signature,
  te.event_position,
  e.stable_event_id,
  te.event_signature,
  e.source_file,
  e.source_line,
  e.result
FROM
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1` te
JOIN
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
JOIN
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` e
  USING (event_signature)
{%- if not loop.last %}
UNION ALL
{% endif -%}
{% endfor %}
