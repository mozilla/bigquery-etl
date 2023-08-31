SELECT
  dag_id,
  map_index,
  run_id,
  task_id,
  content,
  created_at,
  updated_at,
  user_id
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.task_instance_note`
WHERE
  NOT _fivetran_deleted
