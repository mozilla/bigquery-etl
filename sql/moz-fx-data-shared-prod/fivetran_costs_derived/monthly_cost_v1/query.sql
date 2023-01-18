WITH usage_cost AS (
  SELECT
    destination_id,
    CAST(CONCAT(measured_month, "-01") AS DATE) AS measured_month,
    amount AS dollars_spent
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.usage_cost`
),
credits_used AS (
  SELECT
    destination_id,
    CAST(CONCAT(measured_month, "-01") AS DATE) AS measured_month,
    credits_consumed AS credits_spent
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.credits_used`
),
fivetran_monthly_cost AS (
  SELECT
    COALESCE(credits_used.destination_id, usage_cost.destination_id) AS destination_id,
    COALESCE(credits_used.measured_month, usage_cost.measured_month) AS measured_month,
    credits_used.credits_spent,
    usage_cost.dollars_spent
  FROM
    credits_used
  FULL OUTER JOIN
    usage_cost
  USING
    (destination_id, measured_month)
)
SELECT
  *
FROM
  fivetran_monthly_cost
