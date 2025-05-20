CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform_derived.service_subscription_events_v1_live`
AS
WITH subscription_changes AS (
  SELECT
    id AS service_subscriptions_history_id,
    valid_from AS `timestamp`,
    subscription,
    LAG(subscription) OVER (PARTITION BY subscription.id ORDER BY valid_from) AS old_subscription
  FROM
    `moz-fx-data-shared-prod.subscription_platform_derived.service_subscriptions_history_v1`
),
subscription_start_events AS (
  SELECT
    subscription.started_at AS `timestamp`,
    'Subscription Start' AS type,
    CONCAT(
      IF(
        subscription.customer_service_subscription_number = 1,
        'New Customer',
        'Returning Customer'
      ),
      IF(subscription.is_trial, ' Trial', '')
    ) AS reason,
    service_subscriptions_history_id,
    subscription,
    old_subscription
  FROM
    subscription_changes
  QUALIFY
    1 = ROW_NUMBER() OVER (PARTITION BY subscription.id ORDER BY `timestamp`)
),
subscription_end_events AS (
  SELECT
    subscription.ended_at AS `timestamp`,
    'Subscription End' AS type,
    -- TODO: rather than "Unknown", determine if the user cancelled intentionally or their payment failed
    IF(NOT subscription.auto_renew, 'Auto-Renew Disabled', 'Unknown') AS reason,
    service_subscriptions_history_id,
    subscription,
    old_subscription
  FROM
    subscription_changes
  WHERE
    subscription.ended_at IS NOT NULL
    AND old_subscription.ended_at IS NULL
),
mozilla_account_change_events AS (
  SELECT
    `timestamp`,
    'Mozilla Account Change' AS type,
    CASE
      WHEN old_subscription.mozilla_account_id_sha256 IS NULL
        THEN 'Mozilla Account Added'
      WHEN subscription.mozilla_account_id_sha256 IS NULL
        THEN 'Mozilla Account Removed'
      ELSE 'Mozilla Account Changed'
    END AS reason,
    service_subscriptions_history_id,
    subscription,
    old_subscription
  FROM
    subscription_changes
  WHERE
    old_subscription IS NOT NULL
    AND subscription.mozilla_account_id_sha256 IS DISTINCT FROM old_subscription.mozilla_account_id_sha256
),
plan_change_events AS (
  SELECT
    `timestamp`,
    'Plan Change' AS type,
    CASE
      WHEN (
          SELECT
            STRING_AGG(id ORDER BY id)
          FROM
            UNNEST(subscription.other_services)
        ) IS DISTINCT FROM (
          SELECT
            STRING_AGG(id ORDER BY id)
          FROM
            UNNEST(old_subscription.other_services)
        )
        THEN 'Services Changed'
      WHEN (
          SELECT
            STRING_AGG(tier ORDER BY id)
          FROM
            UNNEST(ARRAY_CONCAT([subscription.service], subscription.other_services))
        ) IS DISTINCT FROM (
          SELECT
            STRING_AGG(tier ORDER BY id)
          FROM
            UNNEST(ARRAY_CONCAT([old_subscription.service], old_subscription.other_services))
        )
        THEN 'Service Tier Changed'
      WHEN subscription.plan_interval != old_subscription.plan_interval
        OR subscription.plan_interval_count != old_subscription.plan_interval_count
        THEN 'Plan Interval Changed'
    END AS reason,
    service_subscriptions_history_id,
    subscription,
    old_subscription
  FROM
    subscription_changes
  WHERE
    subscription.provider_plan_id != old_subscription.provider_plan_id
),
trial_change_events AS (
  SELECT
    `timestamp`,
    'Trial Change' AS type,
    IF(subscription.is_trial, 'Trial Started', 'Trial Converted') AS reason,
    service_subscriptions_history_id,
    subscription,
    old_subscription
  FROM
    subscription_changes
  WHERE
    subscription.is_trial != old_subscription.is_trial
    AND subscription.ended_at IS NULL
),
auto_renew_change_events AS (
  SELECT
    `timestamp`,
    'Auto-Renew Change' AS type,
    IF(subscription.auto_renew, 'Auto-Renew Enabled', 'Auto-Renew Disabled') AS reason,
    service_subscriptions_history_id,
    subscription,
    old_subscription
  FROM
    subscription_changes
  WHERE
    subscription.auto_renew != old_subscription.auto_renew
    AND subscription.ended_at IS NULL
),
all_events AS (
  SELECT
    *
  FROM
    subscription_start_events
  UNION ALL
  SELECT
    *
  FROM
    subscription_end_events
  UNION ALL
  SELECT
    *
  FROM
    mozilla_account_change_events
  UNION ALL
  SELECT
    *
  FROM
    plan_change_events
  UNION ALL
  SELECT
    *
  FROM
    trial_change_events
  UNION ALL
  SELECT
    *
  FROM
    auto_renew_change_events
)
SELECT
  CONCAT(
    subscription.id,
    '-',
    FORMAT_TIMESTAMP('%FT%H:%M:%E6S', `timestamp`),
    '-',
    REPLACE(type, ' ', '-')
  ) AS id,
  `timestamp`,
  subscription.service.id AS service_id,
  type,
  reason,
  service_subscriptions_history_id,
  subscription,
  old_subscription
FROM
  all_events
