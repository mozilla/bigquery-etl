WITH run AS (
  SELECT
    task_runs.task_id AS task_id,
    task_runs.run_id AS run_id,
    task_runs.worker_group AS worker_group,
    task_runs.worker_id AS worker_id,
    TIMESTAMP_DIFF(task_runs.resolved, task_runs.started, SECOND) AS duration
  FROM
    `moz-fx-data-shared-prod.fxci.task_runs_v1` AS task_runs
  WHERE
    task_runs.submission_date = @submission_date
),
worker AS (
  SELECT
    worker_cost.zone AS zone,
    worker_cost.instance_id AS instance_id,
    SUM(worker_cost.total_cost) AS total_cost,
    SUM(worker_metrics.uptime) AS uptime,
  FROM
    `moz-fx-data-shared-prod.fxci.worker_metrics_v1` AS worker_metrics
  INNER JOIN
    `moz-fx-data-shared-prod.fxci_derived.worker_costs_v1` AS worker_cost
    ON worker_metrics.project = worker_cost.project
    AND worker_metrics.zone = worker_cost.zone
    AND worker_metrics.instance_id = worker_cost.instance_id
  WHERE
    worker_metrics.submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND @submission_date
    AND worker_cost.usage_start_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND @submission_date
  GROUP BY
    zone,
    instance_id
)
SELECT
  run.task_id AS task_id,
  run.run_id AS run_id,
  @submission_date AS submission_date,
  (run.duration / worker.uptime) * worker.total_cost AS run_cost,
FROM
  run
INNER JOIN
  worker
  ON run.worker_group = worker.zone
  AND run.worker_id = worker.instance_id
WHERE
  worker.uptime > run.duration
GROUP BY
  task_id,
  run_id,
  submission_date,
  run.duration,
  worker.uptime,
  worker.total_cost
