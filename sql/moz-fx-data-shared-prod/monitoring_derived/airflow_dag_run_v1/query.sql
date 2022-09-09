SELECT
  dag_id,
  run_type,
  external_trigger,
  state,
  execution_date,
  start_date,
  end_date,
  _fivetran_deleted AS is_deleted
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.dag_run`
