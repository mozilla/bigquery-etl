SELECT
  dag_id,
  name AS tag_name,
  _fivetran_deleted AS is_deleted
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.dag_tag`
