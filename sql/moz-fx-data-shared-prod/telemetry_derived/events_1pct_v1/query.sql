{% if is_init() %}
  SELECT
    *
  FROM
    telemetry.events
  WHERE
    FALSE
    AND sample_id = 0
{% else %}
  SELECT
    *
  FROM
    telemetry.events
  WHERE
    sample_id = 0
    AND submission_date = @submission_date
{% endif %}
