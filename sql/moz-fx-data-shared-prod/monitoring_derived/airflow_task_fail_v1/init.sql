CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.airflow_task_fail_v1`
AS
SELECT
  dag_id,
  task_id,
  duration,
  execution_date,
  start_date,
  end_date,
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.task_fail`
WHERE
  NOT _fivetran_deleted
