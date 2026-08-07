CREATE OR REPLACE VIEW
  `{{ target_project }}.gecko_trace_aggregates.platform_counts` AS
SELECT
  app_id,
  stable_trace_id,
  trace_signature,
  app_build,
  normalized_os,
  normalized_os_version,
  architecture,
  SUM(hit_count) AS hit_count
FROM (
{% for app_id in applications -%}
  SELECT
    "{{ app_id }}" AS app_id,
    *
  FROM
    `{{ target_project }}.{{ app_id }}_derived.gecko_trace_platform_counts_v1`
  {%- if not loop.last %}
  UNION ALL
  {% endif -%}
{% endfor %}
)
GROUP BY
  app_id,
  stable_trace_id,
  trace_signature,
  app_build,
  normalized_os,
  normalized_os_version,
  architecture
