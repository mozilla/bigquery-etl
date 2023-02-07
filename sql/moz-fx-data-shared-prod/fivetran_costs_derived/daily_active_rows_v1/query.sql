WITH incremental_mar AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.incremental_mar_v1`
),
destinations AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.destinations_v1`
),
daily_active_rows AS (
  SELECT
    destinations.destination_name,
    incremental_mar.measured_date,
    incremental_mar.connector_name,
    SUM(incremental_mar.paid_active_rows) AS paid_active_rows,
    SUM(incremental_mar.free_active_rows) AS free_active_rows,
    SUM(incremental_mar.total_active_rows) AS total_active_rows
  FROM
    incremental_mar
  LEFT JOIN
    destinations
  USING
    (destination_id)
  GROUP BY
    destination_name,
    measured_date,
    connector_name
)
SELECT
  *
FROM
  daily_active_rows
