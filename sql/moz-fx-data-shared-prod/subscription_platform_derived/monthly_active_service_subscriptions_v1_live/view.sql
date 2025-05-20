CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_service_subscriptions_v1_live`
AS
WITH months AS (
  SELECT
    month_start_date,
    LAST_DAY(month_start_date, MONTH) AS month_end_date,
    (LAST_DAY(month_start_date, MONTH) + 1) AS next_month_start_date
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY('2019-10-01', (CURRENT_DATE() - 1), INTERVAL 1 MONTH)
    ) AS month_start_date
),
monthly_active_subscriptions_history AS (
  SELECT
    CONCAT(
      subscriptions_history.subscription.id,
      '-',
      FORMAT_DATE('%Y-%m', months.month_start_date)
    ) AS id,
    months.month_start_date,
    months.month_end_date,
    MIN_BY(
      subscriptions_history,
      subscriptions_history.valid_from
    ) AS earliest_subscription_history,
    MAX_BY(subscriptions_history, subscriptions_history.valid_from) AS latest_subscription_history
  FROM
    months
  JOIN
    `moz-fx-data-shared-prod.subscription_platform_derived.service_subscriptions_history_v1` AS subscriptions_history
    ON TIMESTAMP(months.next_month_start_date) > subscriptions_history.valid_from
    AND TIMESTAMP(months.month_start_date) < subscriptions_history.valid_to
    AND (
      TIMESTAMP(months.month_start_date) < subscriptions_history.subscription.ended_at
      OR subscriptions_history.subscription.ended_at IS NULL
    )
  GROUP BY
    months.month_start_date,
    months.month_end_date,
    subscriptions_history.subscription.id
  HAVING
    LOGICAL_OR(subscriptions_history.subscription.is_active)
)
SELECT
  id,
  month_start_date,
  month_end_date,
  latest_subscription_history.subscription.service.id AS service_id,
  latest_subscription_history.id AS service_subscriptions_history_id,
  latest_subscription_history.subscription,
  (
    earliest_subscription_history.subscription.is_active
    AND earliest_subscription_history.valid_from <= TIMESTAMP(month_start_date)
  ) AS was_active_at_month_start,
  latest_subscription_history.subscription.is_active AS was_active_at_month_end
FROM
  monthly_active_subscriptions_history
