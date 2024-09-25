SELECT
  submission_date,
  reference_project_id,
  reference_dataset_id,
  reference_table_id,
  creation_date,
  task_duration,
  total_terabytes_processed,
  total_terabytes_billed,
  total_slot_ms,
  cost,
  job_id,
  user_email AS service_account,
  REGEXP_EXTRACT(bigeye_query, r'\'metric_id:(\d+)\'') AS metric_id,
  REPLACE(
    REGEXP_EXTRACT(LOWER(bigeye_query), r'`__grain_start__` from `([\w\d_\-\.`]+) as'),
    "`",
    ""
  ) AS monitored_table_id,
FROM
  `moz-fx-data-shared-prod.monitoring.bigquery_usage`
WHERE
  DATE(submission_date) = "2024-09-24"
  AND user_type = "bigeye"
  AND total_slot_ms IS NOT NULL
