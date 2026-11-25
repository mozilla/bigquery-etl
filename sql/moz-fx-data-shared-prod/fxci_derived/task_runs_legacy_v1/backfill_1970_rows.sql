-- One-off, not scheduled. Run once after the daily backfill of task_runs_legacy_v1
-- has covered 2020-08-01 to 2020-09-30.
--
-- About 1.15 million resolved runs of 2020-08 and 2020-09 sit in the 1970-01-01
-- partition of taskclusteretl.pulse_task because their message has no event
-- time. The daily backfill reads one partition per day, so it cannot place them.
-- This inserts them into their real partitions, dated from the run itself, with
-- the same filters as query.sql. Runs already loaded from another message are
-- skipped.
INSERT INTO
  `moz-fx-data-shared-prod.fxci_derived.task_runs_legacy_v1`(
    task_id,
    run_id,
    reason_created,
    reason_resolved,
    resolved,
    scheduled,
    started,
    state,
    submission_date,
    worker_group,
    worker_id
  )
WITH messages AS (
  SELECT
    status.taskId AS task_id,
    runId AS run_id,
    status.runs[SAFE_OFFSET(runId)] AS r,
    workerGroup AS message_worker_group,
    workerId AS message_worker_id
  FROM
    `moz-fx-data-shared-prod.taskclusteretl.pulse_task`
  WHERE
    DATE(TIME) = DATE "1970-01-01"
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
  DATE(COALESCE(r.started, r.scheduled)) AS submission_date,
  COALESCE(r.workerGroup, message_worker_group) AS worker_group,
  COALESCE(r.workerId, message_worker_id) AS worker_id
FROM
  messages
WHERE
  r.state IN ("completed", "failed", "exception")
  AND NOT EXISTS (
    SELECT
      1
    FROM
      `moz-fx-data-shared-prod.fxci_derived.task_runs_legacy_v1` AS loaded
    WHERE
      loaded.submission_date
      BETWEEN DATE "2020-07-01"
      AND DATE "2020-10-31"
      AND loaded.task_id = messages.task_id
      AND loaded.run_id = messages.run_id
  )
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY task_id, run_id ORDER BY r.resolved DESC) = 1
