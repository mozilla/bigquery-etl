CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_dag_run`
AS
SELECT
  * EXCEPT (is_deleted)
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_dag_run_v1`
WHERE
  NOT is_deleted
