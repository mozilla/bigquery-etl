CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_derived.shredder_progress`
AS
WITH shredder AS (
  SELECT
    task_id,
    CASE
      WHEN target = "moz-fx-data-shared-prod.telemetry_stable.main_v4"
        THEN "telemetry_main"
      WHEN target = "moz-fx-data-shared-prod.telemetry_derived.main_summary_v4"
        THEN "telemetry_main_summary"
      ELSE "all"
    END AS airflow_task_id,
    target,
    end_date,
    -- oldest table size
    ARRAY_AGG(STRUCT(target_bytes, source_bytes) ORDER BY _PARTITIONTIME LIMIT 1)[OFFSET(0)].*,
    -- newest job
    ARRAY_AGG(
      STRUCT(
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
  GROUP BY
    task_id,
    target, -- every task has exactly one target
    end_date
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
successful_jobs AS (
  SELECT
    *,
    total_bytes_processed AS bytes_complete,
    total_slot_ms AS slot_ms,
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
    -- assume jobs too old for metadata are complete
    LOGICAL_AND(job_too_old IS TRUE OR end_time IS NOT NULL) AS complete,
    COUNTIF(job_too_old IS TRUE OR end_time IS NOT NULL) AS tasks_complete,
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
