CREATE OR REPLACE VIEW
  `{{gcp_project}}.operational_monitoring.{{slug}}_scalar`
AS

SELECT
  client_id,
  {% if xaxis == "submission_date" %}
  submission_date,
  {% else %}
  build_id,
  {% endif %}
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
  {% if xaxis == "submission_date" %}
    {% if start_date %}
    DATE(submission_date) >= "{{start_date}}"
    {% else %}
    DATE(submission_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    {% endif %}
  {% else %}
    {% if start_date %}
    PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) >= "{{start_date}}"
    {% else %}
    PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    {% endif %}
    AND DATE(submission_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  {% endif %}
GROUP BY
  client_id,
  {% if xaxis == "submission_date" %}
  submission_date,
  {% else %}
  build_id,
  {% endif %}
  cores_count,
  os,
  branch,
  agg_type,
  probe
