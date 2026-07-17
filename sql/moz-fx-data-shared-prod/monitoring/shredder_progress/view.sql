CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.shredder_progress`
AS
WITH sampling_tasks AS (
  SELECT DISTINCT
    REGEXP_REPLACE(task_id, r"__sample_[0-9_]+$", "") AS task_id,
    end_date,
  FROM
    `moz-fx-data-shredder.shredder_state.shredder_state`
  WHERE
    REGEXP_CONTAINS(task_id, r"__sample_[0-9_]+$")
),
shredder AS (
  SELECT
    task_id,
    CASE
      WHEN target LIKE "moz-fx-data-experiments.%"
        THEN "experiments"
      WHEN target = "moz-fx-data-shared-prod.telemetry_stable.main_v5"
        THEN "telemetry_main_v5"
      WHEN target = "moz-fx-data-shared-prod.telemetry_stable.main_use_counter_v4"
        THEN "telemetry_main_use_counter"
      WHEN target = "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1"
        THEN "firefox_desktop_metrics"
      -- previous groups can also have sampling but they're in their own groups
      WHEN sampling_tasks.task_id IS NOT NULL
        THEN "with_sampling"
      ELSE "all"
    END AS airflow_task_id,
    target,
    end_date,
    ARRAY_AGG(STRUCT(target_bytes, source_bytes) ORDER BY target_bytes LIMIT 1)[OFFSET(0)].*,
    -- newest job
    ARRAY_AGG(
      STRUCT(
        -- a task with no shredder_state row for this run had nothing to shred.
        -- since runs don't overlap per airflow task and an active run always has
        -- an in-flight job, such a task is complete once the run is finished.
        job_created IS NULL AS no_work,
        -- job metadata over 28 days old is not queried
        job_created <= TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 28 DAY) AS job_too_old,
        job_created,
        SPLIT(job_id, ":")[OFFSET(0)] AS project_id,
        SPLIT(job_id, ".")[OFFSET(1)] AS job_id
      )
      ORDER BY
        job_created DESC
      LIMIT
        1
    )[OFFSET(0)].*
  FROM
    `moz-fx-data-shredder.shredder_state.tasks`
  LEFT JOIN
    `moz-fx-data-shredder.shredder_state.shredder_state`
    USING (task_id, end_date)
  LEFT JOIN
    sampling_tasks
    USING (task_id, end_date)
  GROUP BY
    task_id,
    target, -- every task has exactly one target
    end_date,
    sampling_tasks.task_id
),
jobs AS (
  SELECT
    creation_time,
    start_time,
    end_time,
    state,
    error_result,
    job_id,
    project_id,
    total_bytes_processed AS bytes_complete,
    total_slot_ms AS slot_ms,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1`
),
successful_jobs AS (
  SELECT
    *
  FROM
    jobs
  WHERE
    creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 28 DAY)
    AND state = "DONE"
    AND error_result IS NULL
),
progress_by_target AS (
  SELECT
    airflow_task_id,
    end_date,
    MIN(IFNULL(start_time, job_created)) AS start_time,
    MAX(end_time) AS end_time,
    -- a task is complete if it had nothing to shred, if its job is too old for
    -- metadata (assumed complete), or if its job finished
    LOGICAL_AND(no_work OR job_too_old IS TRUE OR end_time IS NOT NULL) AS complete,
    COUNTIF(no_work OR job_too_old IS TRUE OR end_time IS NOT NULL) AS tasks_complete,
    COUNT(*) AS tasks_total,
    SUM(bytes_complete) AS bytes_complete,
    -- count target_bytes once per table and source_bytes once per task
    MIN(target_bytes) + SUM(source_bytes) AS bytes_total,
    SUM(slot_ms) AS slot_ms,
    NULLIF(SUM(bytes_complete), 0) / SUM(slot_ms) AS bytes_per_slot_ms,
  FROM
    shredder
  LEFT JOIN
    successful_jobs
    USING (project_id, job_id)
  GROUP BY
    target,
    airflow_task_id,
    end_date
),
avg_bytes_per_slot_ms AS (
  SELECT
    airflow_task_id,
    end_date,
    -- avg weighted by bytes_total
    SUM(bytes_per_slot_ms * bytes_total) / SUM(bytes_total) AS avg_bytes_per_slot_ms,
  FROM
    progress_by_target
  WHERE
    bytes_per_slot_ms IS NOT NULL
  GROUP BY
    airflow_task_id,
    end_date
),
progress AS (
  SELECT
    airflow_task_id,
    end_date,
    LOGICAL_AND(complete) AS complete,
    SUM(slot_ms) AS slot_ms_complete,
    -- estimate slot_ms remaining using bytes_per_slot_ms per table where available
    SUM(bytes_total / IFNULL(bytes_per_slot_ms, avg_bytes_per_slot_ms)) AS slot_ms_total,
    -- diff in seconds to include fractional days
    MIN(start_time) AS start_time,
    TIMESTAMP_DIFF(
      COALESCE(
        MAX(end_time),
        -- start_time of the next airflow run when complete
        ANY_VALUE(MIN(start_time)) OVER (
          PARTITION BY
            airflow_task_id
          ORDER BY
            end_date
          ROWS BETWEEN
            1 FOLLOWING
            AND 1 FOLLOWING
        ),
        -- current time for the last airflow run
        CURRENT_TIMESTAMP
      ),
      MIN(start_time),
      SECOND
    ) AS seconds_complete,
    SUM(bytes_complete) AS bytes_complete,
    SUM(bytes_total) AS bytes_total,
    SUM(tasks_complete) AS tasks_complete,
    SUM(tasks_total) AS tasks_total,
  FROM
    progress_by_target
  LEFT JOIN
    avg_bytes_per_slot_ms AS _
    USING (airflow_task_id, end_date)
  GROUP BY
    airflow_task_id,
    end_date
)
SELECT
  STRUCT(airflow_task_id AS task_id, end_date) AS airflow,
  start_time,
  complete,
  IF(
    complete,
    STRUCT(
      TIMESTAMP_ADD(start_time, INTERVAL seconds_complete SECOND) AS actual,
      NULL AS estimate_by_slot_ms,
      NULL AS estimate_by_bytes,
      NULL AS estimate_by_tasks
    ),
    STRUCT(
      NULL AS actual,
      TIMESTAMP_ADD(
        start_time,
        INTERVAL CAST(
          IFNULL(slot_ms_total / slot_ms_complete, 1) * seconds_complete AS INT64
        ) SECOND
      ) AS estimate_by_slot_ms,
      TIMESTAMP_ADD(
        start_time,
        INTERVAL CAST(bytes_total / bytes_complete * seconds_complete AS INT64) SECOND
      ) AS estimate_by_bytes,
      TIMESTAMP_ADD(
        start_time,
        INTERVAL CAST(tasks_total / tasks_complete * seconds_complete AS INT64) SECOND
      ) AS estimate_by_tasks
    )
  ) AS completion_time,
  slot_ms_total,
  bytes_total / POW(2, 50) AS petabytes_total,
  tasks_total,
FROM
  progress
ORDER BY
  end_date DESC,
  airflow_task_id
