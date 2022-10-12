CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_dag`
AS
SELECT
  * EXCEPT (is_deleted, is_active)
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_dag_v1`
WHERE
  NOT is_deleted
  AND is_active
