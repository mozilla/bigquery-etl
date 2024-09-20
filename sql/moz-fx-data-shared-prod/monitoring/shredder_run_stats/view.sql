CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.shredder_run_stats`
AS
SELECT
  shredder_run_date,
  MIN(start_time) AS start_time,
  MAX(end_time) AS end_time,
  TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), HOUR) AS run_time_hours,
  TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), HOUR) / 24 AS run_time_days,
  SUM(total_slot_ms) / 1000 / TIMESTAMP_DIFF(MAX(end_time), MIN(start_time), SECOND) AS avg_slots,
  CASE
    WHEN task_id LIKE 'moz-fx-data-shared-prod.telemetry_stable.main_v%'
      THEN 'main'
    WHEN task_id LIKE 'moz-fx-data-shared-prod.telemetry_stable.main_use_counter_v%'
      THEN 'main_use_counters'
    WHEN task_id LIKE 'moz-fx-data-experiments.%'
      THEN 'experiments'
    ELSE 'all'
  END AS job_group,
  SUM(tib_processed) AS tib_processed,
  SUM(slot_hours) AS slot_hours,
  COUNT(*) AS num_jobs,
  COUNT(error_reason) AS num_job_errors,
FROM
  `moz-fx-data-shared-prod.monitoring_derived.shredder_per_job_stats_v1`
GROUP BY
  job_group,
  shredder_run_date
ORDER BY
  job_group,
  shredder_run_date DESC
