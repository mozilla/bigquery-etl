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
