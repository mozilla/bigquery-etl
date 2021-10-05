WITH
  _previous AS (
  SELECT
    * EXCEPT(submission_date)
  FROM
    `{{ project_id }}.{{ target_table }}`
  WHERE
    submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND udf.shift_28_bits_one_day(days_sent_metrics_ping_bits) > 0
),
  _current AS (
  SELECT
    *
  FROM
    `{{ project_id }}.{{ app_name }}.clients_metrics_daily` AS m
  WHERE
    submission_date = @submission_date
)
SELECT
  DATE(@submission_date) AS submission_date,
  client_id,
  sample_id,
  _current.n_metrics_ping,
  udf.combine_adjacent_days_28_bits(_previous.days_sent_metrics_ping_bits,
    _current.days_sent_metrics_ping_bits) AS days_sent_metrics_ping_bits,
  {% if app_name in metrics -%}
  {% for metric in metrics[app_name] -%}
    {% if metric.counter %}
    COALESCE(_current.{{metric}}, NULL) AS metric,
    {% else %}
    COALESCE(_current.{{metric}}, _previous.{{metric}}) AS metric,
    {% endif %}
  {% endfor -%}
  {% endif -%}
FROM
  _previous
FULL JOIN
  _current
USING
  (client_id,
    sample_id)
