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
monthly_costs AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.monthly_costs_v1`
),
monthly_costs_per_mar AS (
  SELECT
    destination_id,
    measured_month,
    monthly_costs.dollars_spent / NULLIF(SUM(incremental_mar.active_rows), 0) AS cost_per_mar
  FROM
    incremental_mar
  LEFT JOIN
    monthly_costs
  USING
    (measured_month, destination_id)
  WHERE
    incremental_mar.billing_info = "paid"
  GROUP BY
    measured_month,
    destination_id,
    dollars_spent
),
daily_connector_costs AS (
  SELECT
    destinations.destination_name AS destination,
    incremental_mar.measured_date,
    connector,
    billing_info,
    SUM(active_rows) AS active_rows,
    SUM(
      IF(billing_info = "paid", active_rows, 0)
    ) * monthly_costs_per_mar.cost_per_mar AS cost_in_usd
  FROM
    incremental_mar
  LEFT JOIN
    monthly_costs_per_mar
  USING
    (destination_id, measured_month)
  LEFT JOIN
    destinations
  USING
    (destination_id)
  GROUP BY
    destination,
    measured_date,
    connector,
    billing_info,
    monthly_costs_per_mar.cost_per_mar
)
SELECT
  *
FROM
  daily_connector_costs
