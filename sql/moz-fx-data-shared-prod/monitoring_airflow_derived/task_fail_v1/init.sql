CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_airflow_derived.task_fail_v1`
AS
SELECT
  dag_id,
  task_id,
  duration,
  execution_date,
  start_date,
  end_date,
  _fivetran_deleted AS is_deleted
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.task_fail`
