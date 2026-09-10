CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.traces_daily`
AS
{% for app_id in applications -%}
SELECT
  "{{ app_id }}" AS app_id,
  d.submission_date,
  t.stable_trace_id,
  d.trace_signature,
  d.hit_count,
  d.avg_duration_nano
FROM
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_daily_v1` d
JOIN
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
{%- if not loop.last %}
UNION ALL
{% endif -%}
{% endfor %}
