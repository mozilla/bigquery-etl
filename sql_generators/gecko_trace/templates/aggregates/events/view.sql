CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.events` AS
SELECT
  app_id,
  stable_event_id,
  event_signature,
  source_file,
  source_line,
  result,
  SUM(hit_count) AS hit_count,
  MIN(submission_date) AS first_seen_date,
  MAX(submission_date) AS latest_seen_date
FROM (
{% for app_id in applications -%}
  SELECT
    "{{ app_id }}" AS app_id,
    *
  FROM
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_events_v1`
  {%- if not loop.last %}
  UNION ALL
  {% endif -%}
{% endfor %}
)
GROUP BY
  app_id,
  stable_event_id,
  event_signature,
  source_file,
  source_line,
  result
