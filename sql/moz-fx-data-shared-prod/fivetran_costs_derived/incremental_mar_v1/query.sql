WITH incremental_mar AS (
  SELECT
    measured_date,
    destination_id,
    connector_id,
    free_type,
    incremental_rows
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.incremental_mar`
  UNION ALL
  SELECT
    measured_date,
    destination_id,
    connector_id,
    free_type,
    incremental_rows
  FROM
    `dev-fivetran.fivetran_log.incremental_mar`
)
SELECT
  measured_date,
  DATE_TRUNC(measured_date, month) AS measured_month,
  destination_id,
  connector_id AS connector,
  CASE
    WHEN free_type IN ("PAID", "FREE_RESYNC")
      THEN LOWER(free_type)
    ELSE CONCAT("free_", LOWER(free_type))
  END AS billing_info,
  incremental_rows AS active_rows
FROM
  incremental_mar
