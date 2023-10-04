WITH months AS (
  {% if is_init() %}
    SELECT
      month_start_date,
      LAST_DAY(month_start_date, MONTH) AS month_end_date
    FROM
      UNNEST(
        GENERATE_DATE_ARRAY(
          (
            SELECT
              DATE_TRUNC(DATE(MIN(started_at)), MONTH)
            FROM
              `moz-fx-data-shared-prod.subscription_platform.logical_subscriptions`
          ),
          CURRENT_DATE() - 1,
          INTERVAL 1 MONTH
        )
      ) AS month_start_date
  {% else %}
    SELECT
      DATE_TRUNC(@date, MONTH) AS month_start_date,
      LAST_DAY(@date, MONTH) AS month_end_date
  {% endif %}
),
monthly_active_subscriptions AS (
  SELECT
    CONCAT(
      daily_subscriptions.subscription.id,
      '-',
      FORMAT_DATE('%Y-%m', months.month_start_date)
    ) AS id,
    months.month_start_date,
    months.month_end_date,
    MIN_BY(daily_subscriptions, daily_subscriptions.date) AS earliest_daily_subscription,
    MAX_BY(daily_subscriptions, daily_subscriptions.date) AS latest_daily_subscription
  FROM
    months
  JOIN
    `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_logical_subscriptions_v1` AS daily_subscriptions
  ON
    (daily_subscriptions.date BETWEEN months.month_start_date AND months.month_end_date)
  GROUP BY
    months.month_start_date,
    months.month_end_date,
    daily_subscriptions.subscription.id
)
SELECT
  id,
  month_start_date,
  month_end_date,
  latest_daily_subscription.logical_subscriptions_history_id,
  latest_daily_subscription.subscription,
  (
    earliest_daily_subscription.was_active_at_day_start
    AND earliest_daily_subscription.date = month_start_date
  ) AS was_active_at_month_start,
  latest_daily_subscription.subscription.is_active AS was_active_at_month_end
FROM
  monthly_active_subscriptions
