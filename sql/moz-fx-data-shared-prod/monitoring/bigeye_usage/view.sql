CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigeye_usage`
AS
SELECT
  submission_date,
  reference_project_id,
  reference_dataset_id,
  reference_table_id,
  creation_date,
  task_duration,
  EXTRACT(HOUR FROM task_duration) * 3600 + EXTRACT(MINUTE FROM task_duration) * 60 + EXTRACT(
    SECOND
    FROM
      task_duration
  ) AS task_duration_seconds,
  total_terabytes_processed,
  total_terabytes_billed,
  total_slot_ms,
  cost,
  job_id,
  user_email AS service_account,
  labels,
FROM
  `moz-fx-data-shared-prod.monitoring.bigquery_usage`
WHERE
  user_type = "bigeye"
  AND total_slot_ms IS NOT NULL
