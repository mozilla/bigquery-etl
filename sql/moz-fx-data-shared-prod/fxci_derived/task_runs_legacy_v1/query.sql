-- Task runs before fxci_derived.task_runs_v1 starts (2024-07-24), rebuilt from
-- the legacy taskclusteretl.pulse_task table. pulse_task holds every queue
-- message (defined, pending, running, resolved), so keep the messages whose own
-- run is resolved. The run state is checked, not the task state: the exception
-- message of a retried task is sent while the task is already pending again.
WITH messages AS (
  SELECT
    status.taskId AS task_id,
    runId AS run_id,
    status.runs[SAFE_OFFSET(runId)] AS r,
    workerGroup AS message_worker_group,
    workerId AS message_worker_id,
    DATE(TIME) AS submission_date
  FROM
    `moz-fx-data-shared-prod.taskclusteretl.pulse_task`
  WHERE
    DATE(TIME) = @submission_date
    AND DATE(TIME) < DATE "2024-07-24"
)
SELECT
  task_id,
  run_id,
  r.reasonCreated AS reason_created,
  r.reasonResolved AS reason_resolved,
  r.resolved AS resolved,
  r.scheduled AS scheduled,
  r.started AS started,
  r.state AS state,
  submission_date,
  COALESCE(r.workerGroup, message_worker_group) AS worker_group,
  COALESCE(r.workerId, message_worker_id) AS worker_id
FROM
  messages
WHERE
  r.state IN ("completed", "failed", "exception")
  -- A run resolved just before the cutover can also have been ingested natively
  -- on the first days of task_runs_v1.
  AND NOT EXISTS (
    SELECT
      1
    FROM
      `moz-fx-data-shared-prod.fxci_derived.task_runs_v1` AS native
    WHERE
      native.submission_date
      BETWEEN DATE "2024-07-24"
      AND DATE "2024-07-26"
      AND native.task_id = messages.task_id
      AND native.run_id = messages.run_id
  )
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY task_id, run_id ORDER BY r.resolved DESC) = 1
