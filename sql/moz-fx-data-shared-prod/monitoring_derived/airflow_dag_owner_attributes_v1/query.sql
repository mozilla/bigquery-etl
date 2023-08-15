SELECT
  dag_id,
  owner,
  link,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.dag_owner_attributes`
WHERE
  NOT _fivetran_deleted
