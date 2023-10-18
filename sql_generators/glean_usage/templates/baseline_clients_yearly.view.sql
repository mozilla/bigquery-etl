{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ clients_yearly_view }}`
AS
SELECT
  {% for usage_type, _ in usage_types %}
    `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_{{ usage_type }}_bytes) AS days_since_{{ usage_type }},
    `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(~days_{{ usage_type }}_bytes) AS consecutive_days_{{ usage_type }},
    `moz-fx-data-shared-prod`.udf.bits_to_days_seen(days_{{ usage_type }}_bytes) AS days_{{ usage_type }}_in_past_year,
  {% endfor %}
  DATE_DIFF(submission_date, first_seen_date, DAY) AS days_since_first_seen,
  EXTRACT(DAYOFWEEK FROM submission_date) AS day_of_week,
  *
FROM
  `{{ project_id }}.{{ clients_yearly_table }}`
