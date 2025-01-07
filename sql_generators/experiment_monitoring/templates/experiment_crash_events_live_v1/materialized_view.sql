-- Generated via ./bqetl generate experiment_monitoring
CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.{{ dataset }}_derived.{{ destination_table }}`
  PARTITION BY DATE(partition_date)
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 5)
  AS
  SELECT
    TIMESTAMP_TRUNC(submission_timestamp, DAY) AS partition_date,
    DATE(submission_timestamp) AS submission_date,
    experiment.key AS experiment,
    experiment.value.branch AS branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      -- Aggregates event counts over 5-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
    ) AS window_end,
    {% if dataset == "telemetry" %}
    payload.process_type AS crash_process_type,
    {% else %}
    metrics.string.crash_process_type AS crash_process_type,
    {% endif %}
    COUNT(*) AS crash_count
  FROM
  {% if dataset == "telemetry" %}
    `moz-fx-data-shared-prod.{{ dataset }}_live.crash_v4`
    LEFT JOIN
      UNNEST(environment.experiments) AS experiment
  {% else %}
    `moz-fx-data-shared-prod.{{ dataset }}_live.crash_v1`
    LEFT JOIN
      UNNEST(ping_info.experiments) AS experiment
  {% endif %}
  WHERE
    -- Limit the amount of data the materialized view is going to backfill when created.
    -- This date can be moved forward whenever new changes of the materialized views need to be deployed.
    DATE(submission_timestamp) > '{{ start_date }}'
  GROUP BY
    partition_date,
    submission_date,
    experiment,
    branch,
    window_start,
    window_end,
    crash_process_type
