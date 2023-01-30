SELECT
  measured_date,
  DATE_TRUNC(measured_date, month) AS measured_month,
  destination_id,
  connector_id AS connector_name,
  table_name,
  IF(LOWER(free_type) = "paid", incremental_rows, 0) AS paid_active_rows,
  IF(LOWER(free_type) != "paid", incremental_rows, 0) AS free_active_rows,
  incremental_rows AS total_active_rows
FROM
  `moz-fx-data-bq-fivetran.fivetran_log.incremental_mar`
UNION ALL
SELECT
  measured_date,
  DATE_TRUNC(measured_date, month) AS measured_month,
  destination_id,
  connector_id AS connector_name,
  table_name,
  IF(LOWER(free_type) = "paid", incremental_rows, 0) AS paid_active_rows,
  IF(LOWER(free_type) != "paid", incremental_rows, 0) AS free_active_rows,
  incremental_rows AS total_active_rows
FROM
  `dev-fivetran.fivetran_log.incremental_mar`
