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
    AND task_runs.started IS NOT NULL
    AND task_runs.resolved IS NOT NULL
),
worker_cost AS (
  SELECT
    COALESCE(cloud_provider, "gcp") AS cloud_provider,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT project ORDER BY project), ",") AS project,
    zone,
    instance_id,
    SUM(total_cost) AS total_cost
  FROM
    `moz-fx-data-shared-prod.fxci_derived.worker_costs_v1`
  WHERE
    usage_start_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND @submission_date
    AND (
      cloud_provider IN ("azure", "gcp")
      OR (
        cloud_provider IS NULL
        AND project IN ("fxci-production-level3-workers", "fxci-production-level1-workers")
      )
    )
  GROUP BY
    cloud_provider,
    zone,
    instance_id
),
worker_usage AS (
  SELECT
    cloud_provider,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT project ORDER BY project), ",") AS project,
    zone,
    instance_id,
    SUM(uptime) AS total_uptime,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT attribution_method IGNORE NULLS ORDER BY attribution_method),
      ","
    ) AS attribution_method
  FROM
    `moz-fx-data-shared-prod.fxci_derived.worker_usage_v1`
  WHERE
    usage_start_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND @submission_date
  GROUP BY
    cloud_provider,
    zone,
    instance_id
),
worker_attribution AS (
  SELECT
    worker_cost.cloud_provider,
    worker_cost.project,
    worker_cost.zone,
    worker_cost.instance_id,
    worker_cost.total_cost,
    worker_usage.total_uptime,
    worker_usage.attribution_method
  FROM
    worker_cost
  INNER JOIN
    worker_usage
    USING (cloud_provider, zone, instance_id)
)
SELECT
  run.task_id AS task_id,
  run.run_id AS run_id,
  @submission_date AS submission_date,
  (run.duration / worker_attribution.total_uptime) * worker_attribution.total_cost AS run_cost,
  worker_attribution.cloud_provider AS cloud_provider,
  worker_attribution.project AS worker_project,
  worker_attribution.zone AS worker_zone,
  worker_attribution.instance_id AS worker_instance_id,
  worker_attribution.total_cost AS attribution_worker_cost,
  worker_attribution.total_uptime AS attribution_worker_uptime,
  run.duration AS run_duration,
  worker_attribution.attribution_method AS attribution_method
FROM
  run
INNER JOIN
  worker_attribution
  ON LOWER(run.worker_group) = LOWER(worker_attribution.zone)
  AND LOWER(run.worker_id) = LOWER(worker_attribution.instance_id)
WHERE
  worker_attribution.total_uptime > 0
  AND worker_attribution.total_uptime >= run.duration
