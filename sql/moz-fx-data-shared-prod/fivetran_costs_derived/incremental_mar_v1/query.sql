WITH incremental_mar AS (
  SELECT
    measured_date,
    destination_id,
    connector_id,
    table_name,
    free_type,
    incremental_rows
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.incremental_mar`
)
SELECT
  measured_date,
  DATE_TRUNC(measured_date, month) AS measured_month,
  destination_id,
  connector_id AS connector,
  table_name,
  CASE
    WHEN LOWER(free_type) = "paid"
      OR LOWER(free_type) LIKE "%free%"
      THEN LOWER(free_type)
    ELSE CONCAT("free_", LOWER(free_type))
  END AS billing_type,
  incremental_rows AS active_rows
FROM
  incremental_mar
