CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.airflow_task_reschedule_v1`
AS
SELECT
  dag_id,
  task_id,
  execution_date,
  reschedule_date,
  start_date,
  end_date,
  duration,
  _fivetran_deleted AS is_deleted
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.task_reschedule`
