CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_dag_run`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_dag_run_v1`
