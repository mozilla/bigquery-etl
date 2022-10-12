SELECT
  id,
  pool,
  description,
  slots,
  _fivetran_deleted AS is_deleted
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.slot_pool`
