WITH date_window AS (
  SELECT
    TIMESTAMP(@submission_date) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP(@submission_date), INTERVAL 1 DAY) AS window_end
),
worker_event AS (
  SELECT
    "azure" AS project,
    LOWER(region) AS zone,
    workerId AS instance_id,
    workerPoolId AS worker_pool_id,
    eventType AS event_type,
    timestamp AS event_time,
    CASE
      eventType
      WHEN "instanceBoot"
        THEN 1
      WHEN "workerReady"
        THEN 2
      WHEN "taskStart"
        THEN 3
      WHEN "taskFinish"
        THEN 4
      WHEN "instanceReboot"
        THEN 5
      WHEN "instanceShutdown"
        THEN 6
    END AS event_order
  FROM
    `moz-fx-data-shared-prod.taskclusteretl.worker_metrics`,
    date_window
  WHERE
    timestamp >= TIMESTAMP_SUB(window_start, INTERVAL 1 DAY)
    AND timestamp < TIMESTAMP_ADD(window_end, INTERVAL 1 DAY)
    AND worker = "generic-worker"
    AND workerId LIKE "vm-%"
    AND region IS NOT NULL
    -- taskQueued is logged with the task creation timestamp, not the time the
    -- worker observed the task, so it is not part of the worker lifecycle.
    AND eventType IN (
      "instanceBoot",
      "workerReady",
      "taskStart",
      "taskFinish",
      "instanceReboot",
      "instanceShutdown"
    )
    AND REGEXP_CONTAINS(workerPoolId, r"^(gecko-t|enterprise-t|comm-t)/win")
),
worker_interval AS (
  SELECT
    *,
    LEAD(event_time) OVER (
      PARTITION BY
        worker_pool_id,
        instance_id
      ORDER BY
        event_time,
        event_order
    ) AS next_event_time
  FROM
    worker_event
)
SELECT
  project,
  zone,
  instance_id,
  GREATEST(event_time, window_start) AS interval_start_time,
  LEAST(next_event_time, window_end) AS interval_end_time,
  @submission_date AS submission_date,
  TIMESTAMP_DIFF(
    LEAST(next_event_time, window_end),
    GREATEST(event_time, window_start),
    SECOND
  ) AS uptime
FROM
  worker_interval,
  date_window
WHERE
  event_type != "instanceShutdown"
  AND next_event_time IS NOT NULL
  AND next_event_time > window_start
  AND event_time < window_end
  AND LEAST(next_event_time, window_end) > GREATEST(event_time, window_start)
