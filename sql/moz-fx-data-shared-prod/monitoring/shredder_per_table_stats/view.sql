CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.shredder_per_table_stats`
AS
SELECT
  shredder_run_date,
  SPLIT(task_id, '$')[OFFSET(0)] AS table_id,
  MIN(start_time) AS start_time,
  MAX(end_time) AS end_time,
  SUM(run_time_minutes) / 60 AS run_time_hours,
  SUM(total_slot_ms) / 1000 / GREATEST(
    SUM(TIMESTAMP_DIFF(end_time, start_time, SECOND)),
    1
  ) AS avg_slots,
  SUM(tib_processed) AS tib_processed,
  SUM(slot_hours) AS slot_hours,
  COUNT(*) AS num_jobs,
  COUNT(error_reason) AS num_job_errors,
FROM
  `moz-fx-data-shared-prod.monitoring_derived.shredder_per_job_stats_v1`
GROUP BY
  table_id,
  shredder_run_date
ORDER BY
  slot_hours DESC
