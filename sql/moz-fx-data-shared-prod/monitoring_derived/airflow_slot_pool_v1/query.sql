SELECT
  id,
  pool,
  description,
  slots,
FROM
  `moz-fx-data-bq-fivetran.telemetry_airflow_metadata_public.slot_pool`
WHERE
  NOT _fivetran_deleted
