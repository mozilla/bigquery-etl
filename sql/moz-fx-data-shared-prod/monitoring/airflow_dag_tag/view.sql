CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_dag_tag`
AS
SELECT
  dag_id,
  tag_name
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_dag_tag_v1`
WHERE
  NOT is_deleted
