SELECT
  dag_id,
  task_id,
  duration,
  execution_date,
  start_date,
  end_date,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.task_fail`
WHERE
  NOT _fivetran_deleted
