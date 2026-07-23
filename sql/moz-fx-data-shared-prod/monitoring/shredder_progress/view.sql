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
    -- Initial recorded table size for the run, assuming shredding outpaces ingestion
    -- this used to order by _PARTITIONTIME to take the oldest recording, but that pseudo column
    -- can't be referenced in stage deploys. This is usually equivalent but it's still
    -- close enough even when it's not
    ARRAY_AGG(STRUCT(target_bytes, source_bytes) ORDER BY target_bytes DESC LIMIT 1)[OFFSET(0)].*,
    -- newest job
    ARRAY_AGG(
      STRUCT(
        -- a task with no shredder_state row is either something the run had nothing to
        -- shred for, or a task the run has not reached yet. These are only distinguished
        -- once the run is finished (see run_activity), so no_work alone does not imply
        -- complete for a run that is still writing shredder_state rows.
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
-- A run is considered finished once it stops recording new shredder_state jobs. An
-- in-progress run writes rows continuously (gaps of minutes), while a finished run's
-- most recent job is hours to days old, so a generous 24 hour threshold separates the
-- two. This gates the no_work assumption: only a finished run's tasks with no job can
-- be assumed to have had nothing to shred rather than not being reached yet.
run_activity AS (
  SELECT
    airflow_task_id,
    end_date,
    -- default to finished when a run recorded no jobs at all, so a run that genuinely
    -- had nothing to shred anywhere is not stuck looking incomplete forever
    IFNULL(
      MAX(job_created) < TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 24 HOUR),
      TRUE
    ) AS run_finished,
  FROM
    shredder
  GROUP BY
    airflow_task_id,
    end_date
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
-- Sampled tables are shredded by writing survivors to a per-sample temp table in
-- shredder_tmp and then copying it into place. shredder_state records only the copy
-- job, which reports no bytes or slot_ms, so the query jobs that do the actual work
-- are recovered here. The temp table name encodes the task it belongs to:
-- <dataset>__<table>_<YYYYMMDD>__sample_N_M maps back to
-- moz-fx-data-shared-prod.<dataset>.<table>$<YYYYMMDD>.
shredder_tmp_jobs AS (
  SELECT
    CONCAT(
      "moz-fx-data-shared-prod.",
      REGEXP_EXTRACT(destination_table.table_id, r"^(.+?)__"),
      ".",
      REGEXP_REPLACE(
        REGEXP_EXTRACT(destination_table.table_id, r"^.+?__(.+)__sample_[0-9_]+$"),
        r"_(\d{8})$",
        r"$\1"
      )
    ) AS task_id,
    SUM(total_bytes_processed) AS bytes_complete,
    SUM(total_slot_ms) AS slot_ms,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1`
  WHERE
    creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 28 DAY)
    AND state = "DONE"
    AND error_result IS NULL
    AND destination_table.project_id = "moz-fx-data-shredder"
    AND destination_table.dataset_id = "shredder_tmp"
    AND REGEXP_CONTAINS(destination_table.table_id, r"__sample_[0-9_]+$")
  GROUP BY
    task_id
),
progress_by_target AS (
  SELECT
    airflow_task_id,
    end_date,
    MIN(IFNULL(job.start_time, job_created)) AS start_time,
    MAX(job.end_time) AS end_time,
    -- a task is complete if it had nothing to shred (only trusted once the run is
    -- finished, otherwise the task may just not be reached yet), if its job is too old
    -- for metadata (assumed complete), or if its copy job finished. The copy job is the
    -- authoritative completion signal; a finished shredding query does not mean the
    -- copy has run yet.
    LOGICAL_AND(
      (no_work AND run_finished)
      OR job_too_old IS TRUE
      OR job.end_time IS NOT NULL
    ) AS complete,
    COUNTIF(
      (no_work AND run_finished)
      OR job_too_old IS TRUE
      OR job.end_time IS NOT NULL
    ) AS tasks_complete,
    COUNT(*) AS tasks_total,
    -- copy jobs report no bytes/slot_ms, so fall back to the shredding query jobs
    -- that wrote the survivors to shredder_tmp for the actual work done
    SUM(IFNULL(job.bytes_complete, tmp.bytes_complete)) AS bytes_complete,
    -- count target_bytes once per table and source_bytes once per task
    MIN(target_bytes) + SUM(source_bytes) AS bytes_total,
    SUM(IFNULL(job.slot_ms, tmp.slot_ms)) AS slot_ms,
    NULLIF(SUM(IFNULL(job.bytes_complete, tmp.bytes_complete)), 0) / SUM(
      IFNULL(job.slot_ms, tmp.slot_ms)
    ) AS bytes_per_slot_ms,
  FROM
    shredder
  LEFT JOIN
    successful_jobs AS job
    USING (project_id, job_id)
  LEFT JOIN
    shredder_tmp_jobs AS tmp
    USING (task_id)
  LEFT JOIN
    run_activity
    USING (airflow_task_id, end_date)
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
      -- SAFE guards against overflow when an early run projects an absurd interval;
      -- such an estimate is meaningless anyway and returns NULL instead of failing
      SAFE.TIMESTAMP_ADD(
        start_time,
        INTERVAL CAST(
          IFNULL(slot_ms_total / slot_ms_complete, 1) * seconds_complete AS INT64
        ) SECOND
      ) AS estimate_by_slot_ms,
      SAFE.TIMESTAMP_ADD(
        start_time,
        INTERVAL CAST(bytes_total / bytes_complete * seconds_complete AS INT64) SECOND
      ) AS estimate_by_bytes,
      SAFE.TIMESTAMP_ADD(
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
