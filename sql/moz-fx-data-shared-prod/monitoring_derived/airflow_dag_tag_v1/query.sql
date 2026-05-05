SELECT
  dag_id,
  name AS tag_name,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.dag_tag`
WHERE
  NOT _fivetran_deleted
