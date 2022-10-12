CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.airflow_dag_run_v1`
AS
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
