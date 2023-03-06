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
monthly_paid_mar AS (
  SELECT
    destination_id,
    measured_month,
    SUM(active_rows) AS paid_active_rows
  FROM
    incremental_mar
  WHERE
    billing_type = "paid"
  GROUP BY
    1,
    2
),
monthly_costs_per_mar AS (
  SELECT
    destination_id,
    measured_month,
    monthly_costs.dollars_spent / NULLIF(monthly_paid_mar.paid_active_rows, 0) AS cost_per_mar
  FROM
    monthly_paid_mar
  LEFT JOIN
    monthly_costs
  USING
    (destination_id, measured_month)
),
daily_mar AS (
  SELECT
    destination_id,
    measured_date,
    measured_month,
    connector,
    billing_type,
    SUM(active_rows) AS active_rows,
  FROM
    incremental_mar
  GROUP BY
    1,
    2,
    3,
    4,
    5
),
daily_connector_costs AS (
  SELECT
    destinations.destination_name AS destination,
    daily_mar.measured_date,
    daily_mar.connector,
    daily_mar.billing_type,
    daily_mar.active_rows,
    IF(
      daily_mar.billing_type = "paid",
      daily_mar.active_rows * COALESCE(monthly_costs_per_mar.cost_per_mar, 0),
      0
    ) AS cost_in_usd
  FROM
    daily_mar
  LEFT JOIN
    monthly_costs_per_mar
  USING
    (destination_id, measured_month)
  LEFT JOIN
    destinations
  USING
    (destination_id)
)
SELECT
  *
FROM
  daily_connector_costs
