-- Generated via ./bqetl generate experiment_monitoring
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_crash_rates_live`
AS
WITH crash_events AS (
{% for app_dataset in applications %}
  SELECT
    experiment,
    branch,
    window_start,
    window_end,
    crash_process_type,
    crash_count
  FROM
    `moz-fx-data-shared-prod.{{ app_dataset }}_derived.experiment_crash_events_live_v1`
  WHERE
    window_start > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  UNION ALL
{% endfor %}
SELECT
  experiment,
  branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_crash_aggregates_v1`
WHERE
  window_start <= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
)
SELECT
  crash_events.experiment AS experiment,
  crash_events.branch AS branch,
  window_start,
  window_end,
  crash_process_type,
  crash_count,
  crash_count / enrollments.value AS estimated_crash_rate
  FROM
    crash_events
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_cumulative_population_estimate_v1` AS enrollments
  ON enrollments.time = crash_events.window_start AND
    enrollments.branch = crash_events.branch AND
    enrollments.experiment = crash_events.experiment
  WHERE
    enrollments.time > TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))