CREATE OR REPLACE VIEW
  `{{gcp_project}}.operational_monitoring.{{slug}}_scalar`
AS

SELECT
  client_id,
  build_id,
  {% for dimension in dimensions %}
    {{ dimension.name }},
  {% endfor %}
  branch,
  agg_type,
  name AS probe,
  CASE agg_type
    WHEN "MAX" THEN MAX(value)
    ELSE SUM(value)
  END AS value
FROM `{{gcp_project}}.{{dataset}}.{{slug}}_scalar`
WHERE
  PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  AND DATE(submission_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY
  client_id,
  build_id,
  cores_count,
  os,
  branch,
  agg_type,
  probe
