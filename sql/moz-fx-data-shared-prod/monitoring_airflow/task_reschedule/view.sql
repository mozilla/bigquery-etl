CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_airflow.task_reschedule`
AS
SELECT
  * EXCEPT (is_deleted)
FROM
  `moz-fx-data-shared-prod.monitoring_airflow_derived.task_reschedule_v1`
WHERE
  NOT is_deleted
