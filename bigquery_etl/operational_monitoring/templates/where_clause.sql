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
