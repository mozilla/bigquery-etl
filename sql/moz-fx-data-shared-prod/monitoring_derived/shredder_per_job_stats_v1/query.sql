SELECT
  -- task id example: project.dataset.table_v1$20240102__sample_10
  SAFE.PARSE_DATE(
    "%Y%m%d",
    REGEXP_EXTRACT(task_id, r"\$([0-9]{8})(?:__sample_[0-9]+)?$")
  ) AS partition_date,
  task_id,
  shredder_state.job_id,
  shredder_state.end_date AS shredder_run_date,
  shredder_jobs.start_time,
  shredder_jobs.end_time,
  TIMESTAMP_DIFF(shredder_jobs.end_time, shredder_jobs.start_time, SECOND) / 60 AS run_time_minutes,
  total_slot_ms / 1000 / TIMESTAMP_DIFF(
    shredder_jobs.end_time,
    shredder_jobs.start_time,
    SECOND
  ) AS avg_slots,
  total_bytes_processed,
  total_bytes_processed / 1024 / 1024 / 1024 / 1024 AS tib_processed,
  total_slot_ms,
  total_slot_ms / 1000 / 60 / 60 AS slot_hours,
  shredder_rows_deleted.deleted_row_count,
  shredder_rows_deleted.partition_id,
  REGEXP_REPLACE(task_id, r"__sample_([0-9]+)$", "") AS parent_task_id,
  SAFE_CAST(REGEXP_EXTRACT(task_id, r"__sample_([0-9]+)$") AS INT) AS shredded_sample_id,
  shredder_jobs.error_result.reason AS error_reason,
  shredder_jobs.error_result.message AS error_message,
FROM
  `moz-fx-data-shredder.shredder_state.shredder_state` AS shredder_state
INNER JOIN
  `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1` AS shredder_jobs
  ON (shredder_jobs.job_id = SPLIT(shredder_state.job_id, '.')[OFFSET(1)])
LEFT JOIN
  `moz-fx-data-shared-prod.monitoring_derived.shredder_rows_deleted_v1` AS shredder_rows_deleted
  ON (shredder_jobs.job_id = SPLIT(shredder_rows_deleted.job_id, '.')[OFFSET(1)])
WHERE
  {% if is_init() %}
    shredder_state.job_created >= "2024-01-22"
    AND shredder_jobs.creation_time >= "2024-01-22"
    AND shredder_jobs.end_time IS NOT NULL
  {% else %}
    DATE(shredder_state.job_created)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
    AND DATE(shredder_jobs.creation_time)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND @submission_date
    AND DATE(shredder_jobs.end_time) = @submission_date
  {% endif %}
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY shredder_state.job_id) = 1
