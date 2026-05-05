CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.airflow_dag_tag`
AS
SELECT
  dag_id,
  ARRAY_AGG(tag_name) AS tags
FROM
  `moz-fx-data-shared-prod.monitoring_derived.airflow_dag_tag_v1`
GROUP BY
  dag_id
