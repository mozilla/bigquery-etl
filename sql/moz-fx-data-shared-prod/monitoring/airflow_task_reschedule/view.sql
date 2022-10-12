CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_task_reschedule`
AS
SELECT
  * EXCEPT (is_deleted)
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_task_reschedule_v1`
WHERE
  NOT is_deleted
