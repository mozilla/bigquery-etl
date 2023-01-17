CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fivetran_costs.monthly_costs_per_connector`
AS
WITH monthly_mar_per_connector AS (
  SELECT
    destination_id,
    measured_month,
    connector_name,
    sum(paid_active_rows) AS connector_paid_monthly_active_rows,
    sum(free_active_rows) AS connector_free_monthly_active_rows,
    sum(total_active_rows) AS connector_total_monthly_active_rows
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.incremental_mar_v1`
  GROUP BY
    destination_id,
    connector_name,
    measured_month
),
monthly_mar_total AS (
  SELECT
    destination_id,
    measured_month,
    sum(paid_active_rows) AS month_paid_monthly_active_rows,
    sum(total_active_rows) AS month_total_monthly_active_rows
  FROM
    `moz-fx-data-shared-prod.fivetran_costs_derived.incremental_mar_v1`
  GROUP BY
    destination_id,
    measured_month
),
monthly_costs_per_connector AS (
  SELECT
    destination_name,
    measured_month,
    connector_name,
    connector_paid_monthly_active_rows,
    connector_free_monthly_active_rows,
    connector_total_monthly_active_rows,
    round(
      connector_paid_monthly_active_rows / monthly_mar_total.month_paid_monthly_active_rows * 100,
      2
    ) AS percentage_of_total_paid_mar,
    round(
      connector_paid_monthly_active_rows / monthly_mar_total.month_paid_monthly_active_rows * monthly_cost.dollars_spent,
      2
    ) AS connector_cost_in_usd,
    round(
      connecrot_paid_monthly_active_rows / monthly_mar_total.month_paid_monthly_active_rows * monthly_cost.credits_spent,
      2
    ) AS connector_cost_in_credit
    -- todo translate cost_in_credit to usd
    -- todo get change in usage compared to the previous month for each connector
  FROM
    monthly_mar_per_connector
  LEFT JOIN
    `moz-fx-data-shared-prod.fivetran_costs_derived.destination_v1` AS destination
  USING
    (destination_id)
  LEFT JOIN
    monthly_mar_total
  USING
    (destination_id, measured_month)
  LEFT JOIN
    `moz-fx-data-shared-prod.fivetran_costs_derived.monthly_cost_v1` AS monthly_cost
  USING
    (destination_id, measured_month)
)
SELECT
  *
FROM
  monthly_costs_per_connector
