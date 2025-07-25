CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_service_subscriptions_v1_live`
AS
WITH dates AS (
  SELECT
    `date`,
    (`date` + 1) AS next_date
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2019-10-10', CURRENT_DATE())) AS `date`
),
daily_active_subscriptions_history AS (
  SELECT
    CONCAT(subscriptions_history.subscription.id, '-', FORMAT_DATE('%F', dates.date)) AS id,
    dates.date,
    MIN_BY(
      subscriptions_history,
      subscriptions_history.valid_from
    ) AS earliest_subscription_history,
    MAX_BY(subscriptions_history, subscriptions_history.valid_from) AS latest_subscription_history
  FROM
    dates
  JOIN
    `moz-fx-data-shared-prod.subscription_platform_derived.service_subscriptions_history_v1` AS subscriptions_history
    ON TIMESTAMP(dates.next_date) > subscriptions_history.valid_from
    AND TIMESTAMP(dates.date) < subscriptions_history.valid_to
    AND (
      TIMESTAMP(dates.date) < subscriptions_history.subscription.ended_at
      OR subscriptions_history.subscription.ended_at IS NULL
    )
  GROUP BY
    dates.date,
    subscriptions_history.subscription.id
  HAVING
    LOGICAL_OR(subscriptions_history.subscription.is_active)
)
SELECT
  id,
  `date`,
  latest_subscription_history.subscription.service.id AS service_id,
  latest_subscription_history.id AS service_subscriptions_history_id,
  latest_subscription_history.subscription,
  (
    earliest_subscription_history.subscription.is_active
    AND earliest_subscription_history.valid_from <= TIMESTAMP(`date`)
  ) AS was_active_at_day_start,
  latest_subscription_history.subscription.is_active AS was_active_at_day_end
FROM
  daily_active_subscriptions_history
