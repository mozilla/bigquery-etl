SELECT
  dag_id,
  name AS tag_name,
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.dag_tag`
WHERE
  NOT _fivetran_deleted
