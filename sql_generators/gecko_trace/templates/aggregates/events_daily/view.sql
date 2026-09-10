CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.events_daily`
AS
{% for app_id in applications -%}
SELECT
  "{{ app_id }}" AS app_id,
  d.submission_date,
  e.stable_event_id,
  d.event_signature,
  d.hit_count
FROM
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_daily_v1` d
JOIN
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1` e
  USING (event_signature)
{%- if not loop.last %}
UNION ALL
{% endif -%}
{% endfor %}
