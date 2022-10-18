CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.airflow_dag_tag_v1`
AS
SELECT
  dag_id,
  name AS tag_name,
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.dag_tag`
WHERE
  NOT _fivetran_deleted
