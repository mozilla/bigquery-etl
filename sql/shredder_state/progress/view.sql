CREATE OR REPLACE VIEW
  `moz-fx-data-shredder.shredder_state.progress`
AS
WITH max_end_date AS (
  SELECT
    MAX(end_date) AS end_date
  FROM
    `moz-fx-data-shredder.shredder_state.shredder_state`
),
shredder_state AS (
  SELECT
    task_id,
    -- NEWEST job for each task_id
    ARRAY_AGG(
      STRUCT(
        start_date,
        end_date,
        SPLIT(job_id, ":")[offset(0)] AS project_id,
        SPLIT(job_id, ".")[offset(1)] AS job_id
      )
      ORDER BY
        job_created DESC
      LIMIT
        1
    )[OFFSET(0)].*
  FROM
    `moz-fx-data-shredder.shredder_state.shredder_state`
  -- only for the most recent shredder run
  JOIN
    max_end_date
  USING
    (end_date)
  GROUP BY
    task_id
),
tasks AS (
  SELECT
    task_id,
    ARRAY_AGG(
      STRUCT(start_date, end_date, target, target_bytes, source_bytes)
      -- already filtered on max_end_date, so use OLDEST metadata
      ORDER BY
        _PARTITIONTIME
      LIMIT
        1
    )[OFFSET(0)].*
  FROM
    `moz-fx-data-shredder.shredder_state.tasks`
  JOIN
    max_end_date
  USING
    (end_date)
  GROUP BY
    task_id
),
jobs AS (
  -- https://cloud.google.com/bigquery/docs/information-schema-jobs
  SELECT
    *
  FROM
    `moz-fx-data-shredder.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-bq-batch-prod.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
),
progress_by_task AS (
  SELECT
    task_id,
    start_date,
    end_date,
    total_bytes_processed AS bytes_complete,
    total_slot_ms AS slot_ms,
  FROM
    shredder_state
  JOIN
    jobs
  USING
    (project_id, job_id)
  WHERE
    creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 28 DAY)
    AND state = "DONE"
    AND error_result IS NULL
),
progress_by_target AS (
  SELECT
    MAX(end_date) AS end_date,
    COUNT(progress_by_task.task_id) AS tasks_complete,
    COUNT(*) AS tasks_total,
    SUM(bytes_complete) AS bytes_complete,
    -- don't estimate total bytes if all tasks are complete
    IF(
      COUNT(progress_by_task.task_id) = COUNT(*),
      SUM(bytes_complete),
      -- count target_bytes once per table and source_bytes once per task
      MIN(target_bytes) + SUM(source_bytes)
    ) AS bytes_total,
    SUM(slot_ms) AS slot_ms,
    NULLIF(SUM(bytes_complete), 0) / SUM(slot_ms) AS bytes_per_slot_ms,
  FROM
    tasks
  LEFT JOIN
    progress_by_task
  USING
    (task_id, start_date, end_date)
  GROUP BY
    target
),
avg_bytes_per_slot_ms AS (
  SELECT AS VALUE
    -- avg weighted by bytes_total
    SUM(bytes_per_slot_ms * bytes_total) / SUM(IF(bytes_per_slot_ms IS NOT NULL, bytes_total, NULL))
  FROM
    progress_by_target
),
progress AS (
  SELECT
    SUM(slot_ms) AS slot_ms_complete,
    SUM(slot_ms) + SUM(
      -- estimate time remaining using bytes_per_slot_ms per table where available
      (bytes_total - IFNULL(bytes_complete, 0)) / IFNULL(bytes_per_slot_ms, avg_bytes_per_slot_ms)
    ) AS slot_ms_total,
    -- tasks start 1 day after end_date
    -- diff in seconds to include fractional days
    TIMESTAMP_DIFF(
      current_timestamp,
      TIMESTAMP(DATE_ADD(MAX(end_date), INTERVAL 1 DAY)),
      MINUTE
    ) / 60 / 24 AS days_complete,
    SUM(bytes_complete) AS bytes_complete,
    SUM(bytes_total) AS bytes_total,
    SUM(tasks_complete) AS tasks_complete,
    SUM(tasks_total) AS tasks_total,
  FROM
    progress_by_target
  CROSS JOIN
    avg_bytes_per_slot_ms
)
SELECT
  slot_ms_complete / slot_ms_total * 100 AS days_percent_complete,
  bytes_complete / bytes_total * 100 AS bytes_percent_complete,
  tasks_complete / tasks_total * 100 AS tasks_percent_complete,
  slot_ms_total / slot_ms_complete * days_complete AS days_total,
  bytes_total / 1024 / 1024 / 1024 / 1024 / 1024 AS petabytes_total,
  tasks_total,
FROM
  progress
