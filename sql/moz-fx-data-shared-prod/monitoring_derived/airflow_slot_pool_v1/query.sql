SELECT
  id,
  pool,
  description,
  slots,
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.slot_pool`
WHERE
  NOT _fivetran_deleted
