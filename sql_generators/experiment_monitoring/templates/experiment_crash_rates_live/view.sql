-- Generated via ./bqetl generate experiment_monitoring
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_crash_rates_live`
AS
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
