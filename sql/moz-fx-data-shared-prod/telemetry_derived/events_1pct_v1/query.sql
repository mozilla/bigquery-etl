SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  sample_id = 0
  {% if not is_init() %}
    AND submission_date = @submission_date
  {% else %}
    AND FALSE
  {% endif %}
