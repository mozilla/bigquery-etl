CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_airflow.task_fail`
AS
SELECT
  dag_id,
  task_id,
  duration,
  execution_date,
  start_date,
  end_date
FROM
  `moz-fx-data-shared-prod.monitoring_airflow_derived.task_fail_v1`
WHERE
  NOT is_deleted
