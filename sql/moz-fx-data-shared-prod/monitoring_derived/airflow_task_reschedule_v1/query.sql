SELECT
  dag_id,
  task_id,
  execution_date,
  reschedule_date,
  start_date,
  end_date,
  duration,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.task_reschedule`
WHERE
  NOT _fivetran_deleted
