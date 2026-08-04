CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.trace_events` AS
SELECT
  app_id,
  stable_trace_id,
  stable_event_id,
  SUM(hit_count) AS hit_count,
  MIN(event_position) AS event_position
FROM (
{% for app_id in applications -%}
  SELECT
    "{{ app_id }}" AS app_id,
    *
  FROM
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_trace_events_v1`
  {%- if not loop.last %}
  UNION ALL
  {% endif -%}
{% endfor %}
)
GROUP BY
  app_id,
  stable_trace_id,
  stable_event_id
