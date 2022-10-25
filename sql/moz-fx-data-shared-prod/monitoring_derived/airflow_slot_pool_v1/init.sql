CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.airflow_slot_pool_v1`
AS
SELECT
  id,
  pool,
  description,
  slots,
FROM
  `moz-fx-data-bq-fivetran.airflow_metadata_airflow_db.slot_pool`
WHERE
  NOT _fivetran_deleted
