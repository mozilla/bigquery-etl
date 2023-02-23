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
    destination_id,
    measured_month,
    credits_spent * 2 AS dollars_spent
  FROM
    monthly_costs_credits
  UNION ALL
  SELECT
    destination_id,
    measured_month,
    dollars_spent
  FROM
    monthly_costs_dollars
)
SELECT
  *
FROM
  monthly_costs
