SELECT
  dag_id,
  run_type,
  external_trigger,
  state,
  execution_date,
  start_date,
  end_date,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.dag_run`
WHERE
  NOT _fivetran_deleted
