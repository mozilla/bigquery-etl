CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_slot_pool`
AS
SELECT
  * EXCEPT (is_deleted)
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_slot_pool_v1`
WHERE
  NOT is_deleted
