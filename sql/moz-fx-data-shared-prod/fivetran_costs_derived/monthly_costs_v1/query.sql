WITH monthly_costs_dollars AS (
  SELECT
    destination_id,
    CAST(CONCAT(measured_month, "-01") AS DATE) AS measured_month,
    amount AS dollars_spent
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.usage_cost`
  UNION ALL
  SELECT
    destination_id,
    CAST(CONCAT(measured_month, "-01") AS DATE) AS measured_month,
    amount AS dollars_spent
  FROM
    `dev-fivetran.fivetran_log.usage_cost`
),
monthly_costs_credits AS (
  SELECT
    destination_id,
    CAST(CONCAT(measured_month, "-01") AS DATE) AS measured_month,
    credits_consumed AS credits_spent
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.credits_used`
  UNION ALL
  SELECT
    destination_id,
    CAST(CONCAT(measured_month, "-01") AS DATE) AS measured_month,
    credits_consumed AS credits_spent
  FROM
    `dev-fivetran.fivetran_log.credits_used`
),
monthly_costs AS (
  SELECT
    COALESCE(
      monthly_costs_credits.destination_id,
      monthly_costs_dollars.destination_id
    ) AS destination_id,
    COALESCE(
      monthly_costs_credits.measured_month,
      monthly_costs_dollars.measured_month
    ) AS measured_month,
    COALESCE(
      monthly_costs_dollars.dollars_spent,
      monthly_costs_credits.credits_spent * 2
    ) AS dollars_spent
)
SELECT
  *
FROM
  monthly_costs
