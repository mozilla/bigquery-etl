SELECT
  `timestamp`,
  dag_id,
  warning_type,
  message
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.dag_warning`
WHERE
  NOT _fivetran_deleted
