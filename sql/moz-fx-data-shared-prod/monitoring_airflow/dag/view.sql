CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_airflow.dag`
AS
SELECT
  * EXCEPT (is_deleted, is_active)
FROM
  `moz-fx-data-shared-prod.monitoring_airflow_derived.dag_v1`
WHERE
  NOT is_deleted
  AND is_active
