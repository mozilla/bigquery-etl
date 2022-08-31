CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_airflow_derived.dag_v1`
AS
SELECT
  dag_id,
  IF(is_subdag, SPLIT(dag_id, ".")[OFFSET(0)], dag_id) AS root_dag_id,
  IF(is_subdag, SPLIT(dag_id, ".")[OFFSET(1)], NULL) AS subdag_id,
  is_subdag,
  owners,
  is_active,
  is_paused,
  has_task_concurrency_limits,
  concurrency,
  _fivetran_deleted AS is_deleted
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.dag`
