SELECT
  `timestamp`,
  id,
  filename,
  stacktrace,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.import_error`
WHERE
  NOT _fivetran_deleted
