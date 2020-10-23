WITH
daily AS (
  SELECT
  {{ submission_date }} AS submission_date,
  sample_id,
  client_id,
  {% for measure in measures %}
  {{ measure.sql }} AS {{ measure.name }},
  {% endfor %}
  FROM
  {{ ref }}
  WHERE
  sample_id = 0
  AND {{ submission_date }} = @submission_date
  GROUP BY
  submission_date,
  sample_id,
  client_id ),
previous AS (
  SELECT
  @submission_date AS as_of_date,
  sample_id,
  client_id,
  {% for measure in measures %}{% for usage in measure.usages %}
  days_{{ usage.name }}_bits << (DATE_DIFF(@submission_date, as_of_date, DAY)) AS days_{{ usage.name }}_bits,
  {% endfor %}{% endfor %}
  FROM
    analysis.klukas_{{ name }}_alltime_v3 AS existing
)
SELECT
  @submission_date AS as_of_date,
  sample_id,
  client_id,
  {% for measure in measures %}{% for usage in measure.usages %}
  CONCAT(
    LEFT(previous.days_{{ usage.name }}_bits, LENGTH(previous.days_{{ usage.name }}_bits) - 1),
    RIGHT(previous.days_{{ usage.name }}_bits, 1) | IF({{ measure.name }}{{ usage.sql }}, b'\x01', b'\x00')
  ) AS days_{{ usage.name }}_bits,
  {% endfor %}{% endfor %}
FROM
  daily
FULL JOIN
  previous
USING
  (sample_id, client_id)
