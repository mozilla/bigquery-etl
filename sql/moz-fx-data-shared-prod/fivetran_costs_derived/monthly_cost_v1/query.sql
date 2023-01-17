-- Query for fivetran_costs_derived.monthly_cost_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
WITH usage_cost AS (
  SELECT
    destination_id,
    CAST(concat(measured_month, "-01") AS date) AS measured_month,
    amount AS dollars_spent
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.usage_cost`
),
credits_used AS (
  SELECT
    destination_id,
    CAST(concat(measured_month, "-01") AS date) AS measured_month,
    credits_consumed AS credits_spent
  FROM
    `moz-fx-data-bq-fivetran.fivetran_log.credits_used`
),
fivetran_monthly_cost AS (
  SELECT
    coalesce(credits_used.destination_id, usage_cost.destination_id) AS destination_id,
    credits_used.credits_spent,
    usage_cost.dollars_spent,
    coalesce(credits_used.measured_month, usage_cost.measured_month) AS measured_month
  FROM
    credits_used
  FULL OUTER JOIN
    usage_cost
  ON
    usage_cost.measured_month = credits_used.measured_month
)
SELECT
  *
FROM
  fivetran_monthly_cost
