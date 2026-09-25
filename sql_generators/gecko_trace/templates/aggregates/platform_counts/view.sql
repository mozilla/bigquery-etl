CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.platform_counts`
AS
{% for app_id in applications -%}
SELECT
  "{{ app_id }}" AS app_id,
  p.submission_date,
  t.stable_trace_id,
  p.trace_signature,
  p.app_build,
  p.normalized_os,
  p.normalized_os_version,
  p.architecture,
  p.hit_count
FROM
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_platform_counts_v1` p
JOIN
  `{{ target_project }}.{{ app_id }}_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
{%- if not loop.last %}
UNION ALL
{% endif -%}
{% endfor %}
