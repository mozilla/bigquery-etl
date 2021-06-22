WITH base AS (
  SELECT
    *,
    DATE_TRUNC(DATE(subscription_start_date), MONTH) AS cohort_month,
    mozfun.norm.subscription_months_renewed(
      -- month is timezone sensitive, so use localized datetime to calculate monthly retention
      DATETIME(subscription_start_date, plan_interval_timezone),
      DATETIME(end_date, plan_interval_timezone),
      2 -- grace period in days
    ) AS months_renewed,
  FROM
    all_subscriptions_v1
)
SELECT
  *,
FROM
  base
CROSS JOIN
  UNNEST(GENERATE_DATE_ARRAY(cohort_month, CURRENT_DATE, INTERVAL 1 MONTH)) AS activity_month
  WITH OFFSET AS renewal_period
