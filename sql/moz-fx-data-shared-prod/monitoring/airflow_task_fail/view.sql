CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_task_fail`
AS
SELECT
  dag_id,
  task_id,
  duration,
  start_date,
  end_date
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_task_fail_v1`
