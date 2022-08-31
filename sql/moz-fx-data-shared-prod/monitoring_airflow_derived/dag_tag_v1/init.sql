CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_airflow_derived.dag_tag_v1`
AS
SELECT
  dag_id,
  name AS tag_name,
  _fivetran_deleted AS is_deleted
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.dag_tag`
