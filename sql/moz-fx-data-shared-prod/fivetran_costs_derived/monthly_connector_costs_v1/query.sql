WITH connector_mar AS (
  SELECT
    destination_id,
    measured_month,
    connector_name,
    SUM(paid_active_rows) AS paid_active_rows,
    SUM(free_active_rows) AS free_active_rows,
    SUM(total_active_rows) AS total_active_rows
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.incremental_mar_v1`
  GROUP BY
    destination_id,
    connector_name,
    measured_month
),
total_mar AS (
  SELECT
    destination_id,
    measured_month,
    SUM(paid_active_rows) AS paid_active_rows,
    SUM(total_active_rows) AS total_active_rows
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.incremental_mar_v1`
  GROUP BY
    destination_id,
    measured_month
),
monthly_connector_costs AS (
  SELECT
    destinations.destination_name,
    measured_month,
    connector_mar.connector_name,
    connector_mar.paid_active_rows,
    connector_mar.free_active_rows,
    connector_mar.total_active_rows,
    ROUND(
      connector_mar.paid_active_rows / NULLIF(total_mar.paid_active_rows, 0) * 100,
      2
    ) AS percentage_of_total_paid_mar,
    ROUND(
      connector_mar.paid_active_rows / NULLIF(
        total_mar.paid_active_rows,
        0
      ) * monthly_costs.dollars_spent,
      2
    ) AS cost_in_usd,
    ROUND(
      connector_mar.paid_active_rows / NULLIF(
        total_mar.paid_active_rows,
        0
      ) * monthly_costs.credits_spent,
      2
    ) AS cost_in_credit
    -- todo translate cost_in_credit to usd
    -- todo get change in usage compared to the previous month for each connector
  FROM
    connector_mar
  LEFT JOIN
    `moz-fx-data-shared-prod.fivetran_costs_derived.destinations_v1` AS destinations
  USING
    (destination_id)
  LEFT JOIN
    total_mar
  USING
    (destination_id, measured_month)
  LEFT JOIN
    `moz-fx-data-shared-prod.fivetran_costs_derived.monthly_costs_v1` AS monthly_costs
  USING
    (destination_id, measured_month)
)
SELECT
  *
FROM
  monthly_connector_costs
