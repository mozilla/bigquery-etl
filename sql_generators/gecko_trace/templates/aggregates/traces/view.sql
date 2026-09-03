CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.traces`
AS
SELECT
  app_id,
  stable_trace_id,
  trace_signature,
  SUM(hit_count) AS hit_count,
  MIN(submission_date) AS first_seen_date,
  MAX(submission_date) AS latest_seen_date,
  SUM(avg_duration_nano * hit_count) / SUM(hit_count) AS avg_duration_nano
FROM
  (
    {% for app_id in applications -%}
      SELECT
        "{{ app_id }}" AS app_id,
        *
      FROM
        `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1`
        {%- if not loop.last %}
          UNION ALL
        {% endif -%}
    {% endfor %}
  )
GROUP BY
  app_id,
  stable_trace_id,
  trace_signature