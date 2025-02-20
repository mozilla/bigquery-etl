-- Generated via ./bqetl generate experiment_monitoring
WITH
{% for app_dataset in applications %}
  {% if app_dataset == "telemetry" %}
  {{ app_dataset }} AS (
    SELECT
      submission_timestamp,
      unnested_experiments.key AS experiment,
      unnested_experiments.value AS branch,
      payload.process_type AS crash_process_type,
      COUNT(*) AS crash_count
    FROM
      `moz-fx-data-shared-prod.telemetry_stable.crash_v4`
    LEFT JOIN
      UNNEST(
        ARRAY(SELECT AS STRUCT key, value.branch AS value FROM UNNEST(environment.experiments))
      ) AS unnested_experiments
    GROUP BY
      submission_timestamp,
      experiment,
      branch,
      crash_process_type
  ),
  {% else %}
  {{ app_dataset }} AS (
    SELECT
      submission_timestamp,
      experiment.key AS experiment,
      experiment.value.branch AS branch,
      metrics.string.crash_process_type AS crash_process_type,
      COUNT(*) AS crash_count,
    FROM
      `moz-fx-data-shared-prod.{{ app_dataset }}_stable.crash_v1`
    LEFT JOIN
      UNNEST(ping_info.experiments) AS experiment
    GROUP BY
      submission_timestamp,
      experiment,
      branch,
      crash_process_type
  ),
  {% endif %}
{% endfor %}
all_events AS (
  {% for app_dataset in applications %}
    SELECT
      *
    FROM
      {{ app_dataset }}
    {% if not loop.last %}
      UNION ALL
    {% endif %}
  {% endfor %}
)
SELECT
  experiment,
  branch,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    -- Aggregates event counts over 5-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
  ) AS window_start,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(submission_timestamp, HOUR),
    INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
  ) AS window_end,
  crash_process_type,
  SUM(crash_count) AS crash_count
FROM
  all_events
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type
