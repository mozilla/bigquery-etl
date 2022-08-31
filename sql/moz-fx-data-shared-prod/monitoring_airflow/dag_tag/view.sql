CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring_airflow.dag_tag`
AS
SELECT
  dag_id,
  tag_name
FROM
  `moz-fx-data-shared-prod.monitoring_airflow_derived.dag_tag_v1`
WHERE
  NOT is_deleted
