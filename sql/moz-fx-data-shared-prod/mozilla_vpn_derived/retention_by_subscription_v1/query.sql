WITH invoices AS (
  SELECT
    lines.subscription AS subscription_id,
    DATE(TIMESTAMP_SECONDS(lines.period.start)) AS period_start_date,
    invoices.billing_reason,
    invoices.status_transitions,
  FROM
    `moz-fx-data-shared-prod`.stripe_external.invoices_v1 AS invoices
  CROSS JOIN
    UNNEST(lines) AS lines
  WHERE
    status IN ("paid", "open")
),
cohort AS (
  SELECT DISTINCT
    subscription_id,
    DATE_TRUNC(MIN(period_start_date), MONTH) AS cohort_month,
  FROM
    invoices
  GROUP BY
    subscription_id
  HAVING
    cohort_month >= "2020-07-01"
),
attribution AS (
  SELECT
    subscription_id,
    product_name,
    plan_amount,
    plan_currency,
    plan_id,
    plan_interval,
    plan_interval_count,
    pricing_plan,
    provider,
    country,
    country_name,
    coarse_attribution_category,
    attribution_category,
    utm_source,
    utm_medium,
    utm_content,
    utm_campaign,
    normalized_medium,
    normalized_source,
    normalized_campaign,
    normalized_content,
    normalized_acquisition_channel,
    website_channel_group,
  FROM
    all_subscriptions_v1
),
cohorts_expanded AS (
  SELECT
    *,
    DATE_DIFF(activity_month, cohort_month, MONTH) AS renewal_period,
  FROM
    cohort
  -- inner join to only include valid vpn subscriptions as defined by all_subscriptions_v1
  JOIN
    attribution
  USING
    (subscription_id)
  CROSS JOIN
    UNNEST(GENERATE_DATE_ARRAY(cohort_month, CURRENT_DATE, INTERVAL 1 MONTH)) AS activity_month
),
renewals AS (
  SELECT DISTINCT
    subscription_id,
    DATE_TRUNC(period_start_date, MONTH) AS activity_month,
  FROM
    invoices
  WHERE
    billing_reason IN ("subscription_create", "subscription_cycle")
    AND status_transitions.paid_at IS NOT NULL
)
SELECT
  *,
  renewals.subscription_id AS renewal_subscription_id,
  renewals.activity_month AS activity_period_month,
FROM
  cohorts_expanded
LEFT JOIN
  renewals
USING
  (subscription_id, activity_month)
