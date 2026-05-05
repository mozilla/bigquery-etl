WITH run AS (
  SELECT
    task_runs.task_id AS task_id,
    task_runs.run_id AS run_id,
    task_runs.worker_group AS worker_group,
    task_runs.worker_id AS worker_id,
    TIMESTAMP_DIFF(task_runs.resolved, task_runs.started, SECOND) AS duration
  FROM
    `moz-fx-data-shared-prod.fxci_derived.task_runs_v1` AS task_runs
  WHERE
    task_runs.submission_date = @submission_date
),
worker_cost AS (
  SELECT
    zone,
    instance_id,
    SUM(total_cost) AS total_cost,
  FROM
    `moz-fx-data-shared-prod.fxci_derived.worker_costs_v1`
  WHERE
    usage_start_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND @submission_date
  GROUP BY
    zone,
    instance_id
),
worker_metric AS (
  SELECT
    zone,
    instance_id,
    SUM(uptime) AS total_uptime
  FROM
    `moz-fx-data-shared-prod.fxci_derived.worker_metrics_v1`
  WHERE
    submission_date
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
  (run.duration / worker_metric.total_uptime) * worker_cost.total_cost AS run_cost,
FROM
  run
INNER JOIN
  worker_cost
  ON run.worker_group = worker_cost.zone
  AND run.worker_id = worker_cost.instance_id
INNER JOIN
  worker_metric
  ON run.worker_group = worker_metric.zone
  AND run.worker_id = worker_metric.instance_id
WHERE
  worker_metric.total_uptime > run.duration
GROUP BY
  task_id,
  run_id,
  submission_date,
  run.duration,
  worker_cost.total_cost,
  worker_metric.total_uptime
