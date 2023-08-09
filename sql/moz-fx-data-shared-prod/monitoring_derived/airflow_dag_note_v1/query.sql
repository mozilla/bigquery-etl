SELECT
  dag_run_id,
  content,
  created_at,
  updated_at,
  user_id,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.dag_run_note`
WHERE
  NOT _fivetran_deleted
